# Benchmarks

Reproducible latency and throughput measurements of the venuewire pipeline,
driven entirely by committed replay fixtures — no network, no credentials, no
Docker. Run them yourself with:

```sh
make bench          # cargo run --release --bin vwbench
```

## Methodology

`vwbench` (`bins/vwbench`) replays the committed NDJSON fixtures through the
**real** normalization, `PipelineState`, and axum serving code — the same code
paths the daemon runs — and reports three things:

1. **Pipeline ingest→publish latency.** For each tick: time `PipelineState::apply`
   (book upsert → match-view recompute → divergence check) plus the
   `tokio::sync::broadcast` publish. This is venuewire's own processing cost.
   Venue-network and socket-read time are *excluded* — replay cannot reproduce
   them — so this isolates the engine's contribution.
2. **Loopback ingest→client-receive latency.** A real axum `/ws` server with
   *N* subscribed WebSocket clients. Each tick is stamped with a fresh `recv_ts`
   at publish; the monitor client computes `now − recv_ts` on receipt. This is
   the full serving path: JSON-encode → WS frame → TCP loopback → JSON-decode.
   Measured with **1** and **50** clients to show fan-out cost.
3. **Throughput.** Max-speed replay through the full pipeline, sustained
   ticks/sec, with the ClickHouse sink path **off** and **on** (a bounded
   `mpsc` drained by a background thread models the daemon's `try_send`-to-sink
   cost without a live ClickHouse).

Fixture ticks are looped to a ~50k-tick workload so percentiles are stable;
loopback uses a 3,000-tick paced stream (200µs inter-publish) to avoid
self-inflicted queueing.

### Environment

| | |
|---|---|
| Hardware | Apple M5, 10 cores |
| OS | macOS 26.5.1 |
| Rust | rustc 1.97.1, `--release` (opt-level 3, `debug = 1`) |
| Fixtures | `polymarket-ws-sample.ndjson` (600 ticks) + `kalshi-rest-sample.ndjson` (272 ticks) |
| Workload | 872 fixture ticks × 58 reps = 50,576 ticks (latency/throughput); 3,000 paced (loopback) |

## Results

Representative run (numbers are stable to ~±10% across runs; the pipeline hot
path is sub-microsecond, so its tail is dominated by occasional allocator/OS
scheduling noise).

### Pipeline ingest→publish (apply + broadcast)

| metric | value |
|---|---|
| p50 | **0.2 µs** |
| p95 | 0.2 µs |
| p99 | 0.2–1.3 µs |
| max | 22–38 µs |
| mean | 0.2 µs |

The synchronous hot path (D14) does one DashMap upsert, a match-index read, and
a lock-free broadcast send — no allocation beyond the published clone, no await.
p50/p95 sit at the timer's resolution floor; the rare tens-of-µs max is a
scheduler or allocator hiccup, not steady-state cost.

### Loopback ingest→client-receive

| clients | p50 | p95 | p99 | max |
|---|---|---|---|---|
| 1  | **75–77 µs** | 96–97 µs | 111–114 µs | 0.2–0.9 ms |
| 50 | **378–380 µs** | 515–552 µs | 649–728 µs | 0.9–2.9 ms |

Serving one client end-to-end (encode → loopback TCP → decode) is ~75µs p50.
Fanning out to 50 clients raises p50 to ~380µs: the publisher serializes and
writes each frame per session, so cost scales with subscriber count — the
expected shape, and the design's deliberate trade (a slow client lags its own
broadcast receiver, D17, rather than backpressuring ingest).

### Throughput

| sink | ticks/sec |
|---|---|
| off | **~7.3–7.5 M** |
| on  | **~2.1–2.2 M** |

With the sink path off, the pipeline sustains ~7.4M ticks/sec — book state and
match/divergence evaluation are not the bottleneck. Enabling the sink `try_send`
per tick drops it to ~2.2M ticks/sec: serializing to `SinkEvent` and the channel
hand-off dominate. Both are far above any real venue's tick rate (the committed
fixtures captured ~400 ticks/sec live), so headroom is ample; the sink path is
where future optimization would pay off if a venue ever approached these rates.

## Interpreting the numbers

- **The engine is not the bottleneck.** Sub-µs per-tick processing and
  multi-million ticks/sec mean venuewire's own logic adds negligible latency;
  end-to-end latency is dominated by the network (live) or the serving/serialize
  path (loopback).
- **Fan-out cost is linear and isolated.** 50 clients cost ~5× one client at
  p50, and — critically — a slow consumer never slows ingest (D17).
- **What's excluded.** Live venue WSS round-trip and socket-read time are not
  measured here (replay can't reproduce them); the daemon's
  `vw_ingest_to_publish_seconds` histogram captures the real recv→publish figure
  in production, viewable on `/metrics`.
