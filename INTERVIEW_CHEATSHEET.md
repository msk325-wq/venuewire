# venuewire — Interview Cheat Sheet

*Private study doc — NOT in the public repo. Goal: be able to walk through and
defend any layer of venuewire live. Read the code alongside this; this is the
map, not a substitute for knowing the territory.*

---

## 0. The 30-second pitch (say this cold)

> venuewire is a real-time market-data engine for prediction markets. It holds
> live WebSocket connections to Kalshi and Polymarket, normalizes their two
> different formats into one internal "tick" schema, figures out which contracts
> on the two venues are about the same real-world event, keeps a live best-price
> view in memory, detects when the venues disagree on a price ("divergence"),
> and serves all of it over REST + WebSocket with Prometheus metrics. It persists
> tick history to ClickHouse and mirrors hot state to Redis. Built in Rust to
> learn production streaming-systems engineering — backpressure, distributed
> state, graceful degradation, and reproducible performance measurement.

## 1. The data flow (draw this if there's a whiteboard)

```
Kalshi WSS ─┐
Polymarket WSS ─┤→ connectors → [bounded mpsc channel] → ingest consumer ──┬→ PipelineState (book + match views + divergence)
Replay fixtures ─┘   (normalize to Tick,                                    │      │
                      stamp recv_ts)                                        │      ├→ Redis mirror (write-behind, 250ms)
                                                                            │      └→ (divergence events)
                                                                            ├→ ClickHouse sink (batched)
                                                                            └→ broadcast → WebSocket fan-out → clients
        matcher (off hot path, timer) → matches.yaml → feeds PipelineState        REST snapshots + /metrics
```

- **7 library crates** (`core`, `connectors`, `matcher`, `state`,
  `sink-clickhouse`, `server`, `recorder`) + **3 binaries** (`venuewire` daemon,
  `vwtap` CLI consumer, `vwbench` benchmark). ~131 test functions.

## 2. Key design decisions — the "why" behind each (this is what they probe)

These map to the D1–D21 log in `docs/decisions.md`. Know the top ~8 cold.

| # | Decision | Why (the defensible reason) | What you rejected |
|---|---|---|---|
| **D1** | `rust_decimal::Decimal` for all prices, never `f64` | Prices/probabilities must be exact — `0.03` isn't representable in float, and equality/threshold checks must be reproducible bit-for-bit | `f64` (rounding noise), integer ticks (venue-specific scale everywhere) |
| **D5** | Bounded channel + **per-instrument conflation** on overflow | Market data is last-value: under backpressure you want the *freshest* price per instrument, not the oldest. Never drop-oldest (loses arbitrary data incl. status), never unbounded (OOM), never block (backpressures the socket read → trips venue heartbeat → disconnect storm) | drop-oldest, unbounded channel, blocking send |
| **D14** | DashMap book, **fully synchronous hot path** | `apply()` never awaits, so no lock is ever held across an await point *by construction*; contention limited to one DashMap shard. Sharded map = concurrent reads/writes without a global lock | single Mutex<HashMap> (global contention), async hot path (await-holding-lock risk) |
| **D15** | Redis mirror is **write-behind, drops on outage** | Redis is a mirror, not source of truth. Hot path only marks a dirty flag (no I/O). A background task flushes every 250ms. On failure: drop the batch, count it, log once — next tick re-marks it dirty anyway. A Redis outage must never stall ingestion | retry queue (grows unbounded / pushes stale data on recovery), synchronous write |
| **D17** | WS fan-out via `tokio::broadcast`, **lagged clients continue** | `broadcast::send` never blocks/fails, so one slow client can't backpressure ingest. A lagging client gets a `{"op":"lagged","missed":n}` frame and resumes from the oldest retained event | per-client bounded queues w/ disconnect (punishes a briefly-slow client; top-of-book ticks are self-healing anyway) |
| **D18** | ClickHouse via **JSONEachRow over HTTP** (reqwest), not the `clickhouse` crate | No heavy new dep; payloads are human-readable (a failed batch replays with `curl`); ClickHouse's actual error text surfaces in the HTTP body; trivial to fake in tests | `clickhouse` crate's RowBinary (pins its own hyper stack, opaque wire format) |
| **D19** | Sink outage: bounded buffer, **drop-oldest-batch**, counted | For time-series, after a long outage the *freshest* rows matter most; the recorder's NDJSON is the durable backfill record. Cap at ~100k rows, drop oldest, count the drops | drop-newest, block (would stall ingest) |
| **D16** | Divergence debounce keyed to **last *emitted* spread** | Comparing to last *emitted* (not last observed) spread means a slow creep of +0.002/tick can't ratchet past the debounce, while a real 1-cent widening always emits immediately | debounce on observed spread (slow-creep leak), fixed cooldown (misses real widenings) |

## 3. The benchmark numbers — state them honestly

- **~7M ticks/sec** = *processing capacity*, measured by replaying recorded
  fixtures at max speed (no network pacing). It's **headroom**, not live venue
  rate — real venues send far less. It proves the engine won't be the bottleneck
  during a volatility spike.
- **With the ClickHouse sink engaged it's ~2.2M/sec** — the sink `try_send` +
  serialization is the cost; the in-memory hot path itself is the fast part.
- **Latency: ~0.2µs p50, ~1µs p99, "ingest→publish"** = from *frame read off the
  socket* → *published to the broadcast bus*. **Does NOT include** the venue's
  network delivery time. If asked: "that's the internal processing hot path;
  venue round-trip is separate and measured by the live histogram."
- **Loopback** (through a real WS server, client timestamps receipt): ~77µs p50 /
  1 client, ~380µs p50 / 50 clients — fan-out cost scales with subscribers, as
  expected.
- All reproducible: `make bench` (the `vwbench` binary). This reproducibility —
  via the record/replay harness — is itself the point: numbers you can't
  reproduce aren't numbers.

## 4. Likely questions → crisp answers

**"Why ClickHouse *and* Redis?"**
Different jobs. Redis = fast access to *current* state (in-memory, "what's the
price now"). ClickHouse = durable, queryable *history* (columnar, built for
time-series analytics, "show me every tick for this instrument last week"). Now
vs. forever.

**"How do you handle backpressure / a slow consumer?"**
Two places. (1) Ingest: bounded channel with per-instrument conflation (D5) —
keep latest per instrument, never block the socket reader. (2) Serving:
broadcast fan-out where a slow WS client lags its own receiver and gets a
`lagged` frame (D17) — it never backpressures ingest.

**"What happens if ClickHouse or Redis goes down?"**
Ingestion keeps running. Sink buffers with drop-oldest and counts drops (D19);
Redis writes are best-effort, dropped and counted, logged once per outage
transition (D15). Downstream outages never stall the live path. (I actually
tested this — ran the daemon with neither service up; ticks kept flowing.)

**"Why Rust?"**
Performance-critical hot path with no GC pauses, and the type system makes the
concurrency safe — e.g. the sync hot path means the compiler guarantees I never
hold a lock across an await. Also what this domain (exchanges, HFT infra) uses.

**"How do you match contracts across venues?"**
Off the hot path (a timer), never blocking ticks. Rule pass: normalize titles,
extract anchors (dates, thresholds), exact key collision → `Exact` match, but
*conservative* — requires anchors + guards against ambiguous/near-miss matches
so it produces zero false exacts (D11). Weaker lexical similarity only reaches
`Review` unless an LLM adjudicates (D12). All persisted in a human-editable
`matches.yaml` with manual-override semantics (D13).

**"How is the hot path actually fast?"**
`PipelineState::apply` is synchronous: one clock read, one DashMap upsert
(rejecting out-of-order ticks by seq), an optional dirty-flag insert for the
mirror, and — only for matched instruments — a brief index read + per-match
recompute + divergence check. No allocation-heavy work, no awaits (D14).

**"What's the record/replay harness and why does it matter?"**
The recorder writes raw venue frames to NDJSON. The replay connector feeds them
back through the *exact same* normalization code as live. So tests and
benchmarks run deterministically, offline, with no credentials — same input,
same result every time. It's how the perf numbers are trustworthy and the
integration tests are reliable.

**"How would you scale this to more venues / more load?"**
The `VenueConnector` trait means a new venue is a new connector impl feeding the
same channel — no changes downstream. For load: the bottleneck is the sink
(2.2M vs 7M), so batch/parallelize the ClickHouse writes; the broadcast fan-out
would move to a dedicated pub/sub (NATS/Kafka) if client count grew — which is
in the v2 parking lot in the docs.

## 5. Honest framing (memorize the posture, not a script)

- **How it was built:** "I designed and directed it, built AI-assisted, and I
  understand every layer and every decision." True, defensible, on-brand at an
  AI-native company. Do **not** imply months of solo hand-coding.
- **If handed a blank editor:** be calibrated to what you can actually write.
  Strengths to lean on: you understand the *architecture and tradeoffs* deeply.
  If Rust syntax under pressure isn't your strength yet, say "I'd reach for the
  pattern I used in venuewire — let me talk through it" and reason out loud.
- **The event-hedge-lab story (if asked):** frame as *curiosity + rigor*, not a
  failed venture. "I wanted to test whether Kalshi contracts could hedge equity
  risk, built the tooling to measure it on real data, and the honest answer was
  'mostly no, because of basis risk' — which is what pulled me deeper into the
  space." Never "I tried to start a company."
- **Biggest interview risk:** being asked to explain/modify the code live and
  freezing. Mitigation: actually read the code. Priorities below.

## 6. What to actually read before an interview (in priority order)

1. `crates/state/src/lib.rs` + `book.rs` — the hot path (`PipelineState::apply`,
   `BookState::apply_tick`). Most likely deep-dive target.
2. `crates/connectors/src/conflate.rs` — the conflation/backpressure logic (D5).
3. `crates/server/src/ws.rs` — the broadcast fan-out + lagged handling (D17).
4. `crates/sink-clickhouse/src/lib.rs` — batching + outage policy (D19).
5. `docs/decisions.md` — skim all 21; know D1, D5, D14, D15, D17, D18 cold.
6. `bins/vwbench/src/main.rs` — how the numbers are actually measured.

Run it yourself once before any call: `cargo run --bin vwtap -- ticks --replay
fixtures/committed/polymarket-ws-sample.ndjson --speed max` and `make bench`, so
you've *seen* it work and can describe the live behavior from memory.
