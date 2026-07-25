# Decision Log

Running ADR log: one paragraph per decision — what, why, what was rejected.
Newest entries at the bottom.

## D1 — `rust_decimal::Decimal` for all prices (M0)

All prices and probabilities are `rust_decimal::Decimal`, never `f64`. Kalshi
cents (0–100) and Polymarket 0–1 token prices normalize into exact decimal
probabilities with no representation error, so equality checks, divergence
thresholds, and stored values are reproducible bit-for-bit. Rejected: `f64`
(0.03 is not representable; spreads accumulate noise) and bare integer ticks
(would force a venue-specific scale through every interface; may revisit as an
internal optimization if Decimal shows up in profiles).

## D2 — figment for config, TOML + `VW_` env overrides (M0)

Config is a serde struct loaded by figment from `config/default.toml` with
`VW_*` environment overrides (`__` for nesting, e.g. `VW_SERVER__BIND`). Every
section has in-code `Default` impls, so a missing file or partial file still
yields a runnable config, and `deny_unknown_fields` catches typos loudly.
Rejected: config-rs (similar; figment's provider model and test `Jail` are
nicer) and hand-rolled env parsing (no layering).

## D3 — Cargo workspace split by runtime role (M0)

One crate per architectural stage (`core`, `connectors`, `matcher`, `state`,
`sink-clickhouse`, `server`, `recorder`) plus thin binaries. Dependencies point
one way — everything depends on `core`, binaries depend on stages — which keeps
the hot tick path (`connectors` → `state`) compilable without HTTP/DB deps and
makes per-crate test scopes obvious. Rejected: a single crate with modules
(compile-time coupling, tangled features) and premature plugin architecture
(two venues in v1; a `VenueConnector` trait is enough).

## D4 — Connector events as one enum over a single bounded channel (M0)

Connectors emit `ConnectorEvent::{Tick, Instrument, Status}` through one
bounded `mpsc` channel rather than separate channels per event kind. Ordering
between a status change and the ticks around it is preserved for free, and the
consumer has a single place to apply backpressure policy (per-instrument
conflation, defined in M1). Rejected: separate channels (reordering between
metadata and ticks; three backpressure policies to reason about).

## D5 — Bounded channel + per-instrument tick conflation on overflow (M1 prep)

The connector event channel stays bounded (default 10_000). When it fills,
`ConflatingSender` moves ticks into an overflow buffer keeping only the latest
tick per instrument — market data has last-value semantics, so under pressure
the freshest quote for every instrument survives while intermediate updates
merge away (counted, exported as `vw_conflation_events_total` in M6). Rare,
order-sensitive `Instrument`/`Status` events are buffered FIFO and never
dropped; the buffer drains back into the channel, backlog before new events,
as capacity frees. Rejected: unbounded channel (a slow consumer grows memory
without limit and hides backpressure until OOM), drop-oldest (silently loses
arbitrary events including status/metadata, and a downstream can end on a
stale price forever), and blocking `send().await` (backpressures the socket
read loop, which trips the venue's heartbeat and turns a slow consumer into a
disconnect storm).

## D6 — Kalshi WSS auth + degraded REST-polling mode (M1)

Kalshi's WebSocket requires an authenticated handshake even for "public"
market-data channels (live-verified: unauthenticated upgrade → HTTP 401; see
docs/research/kalshi-api.md §2), contradicting spec §5.2's assumption. The
connector implements the documented RSA-PSS-SHA256 API-key signing
(`KALSHI-ACCESS-KEY`/`-TIMESTAMP`/`-SIGNATURE` over `ts_ms + "GET" + path`),
keys via `KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PATH` (or inline
`KALSHI_PRIVATE_KEY_PEM`); key material never appears in logs, `Debug`, or
error messages. When credentials are absent the connector logs one clear
warning and degrades to re-polling the unauthenticated REST `/markets`
endpoint every `[kalshi].poll_secs` (edge-cached ~15s upstream, so polling is
cheap and polite), emitting snapshot ticks so the whole pipeline demos with
zero credentials; three consecutive auth-rejected handshakes also downgrade to
polling rather than hammering the venue. Rejected: hard-failing without creds
(kills demos/CI), and silently retrying 401s forever (wasteful, hides
misconfiguration).

## D7 — Ticker channel has no `seq`: gap detection redefined for Kalshi (M1)

Kalshi's `ticker` channel payloads carry no sequence number — `seq` exists
only on the per-`sid` orderbook channels (docs/research/kalshi-api.md §2), so
spec §5.1's sequence/gap detection cannot be implemented for the M1 ticker
subscription; `Status::GapDetected` stays in the contract but is never emitted
by the Kalshi ticker path. Liveness is covered instead by the staleness
watchdog (`[ingest].staleness_reconnect_secs`, default 30s) keyed off every
inbound frame including Kalshi's protocol-level pings every ~10s, giving a
worst-case ~3 missed heartbeats before a forced reconnect + fresh REST
snapshot; missed ticks between sessions are healed by that snapshot since
ticker data is last-value semantics. Real `seq` tracking (and the cheap
`update_subscription`/`get_snapshot` recovery primitive) lands if/when the
orderbook_delta channel is subscribed (v2 depth work). Rejected: inventing
gap detection from `ts_ms` ordering (timestamps are per-market and not
gapless) and subscribing orderbook_delta in v1 just for its seq (top-of-book
only needs ticker).

## D8 — Polymarket market universe selected by Gamma tag slugs (M2)

`[polymarket].categories` became `[polymarket].tags`: live Gamma market
objects carry no usable `category` field (present but null/absent — see
docs/research/polymarket-api.md §1.1), so category-style filtering must go
through Polymarket's tag system. Config takes human-readable tag *slugs*
(`"politics"`, `"economics"`); at startup the connector resolves each slug to
its numeric id via `GET /tags/slug/{slug}` (cached for the process lifetime)
and pages `GET /markets?tag_id=…&active=true&closed=false` ordered by 24h
volume, keeping only `enableOrderBook && acceptingOrders` markets up to
`max_markets` across all tags. Unknown slugs warn and are skipped rather than
failing discovery for the other tags. Rejected: keeping the `categories` name
(misleading — these are venue tag slugs, not our canonical categories),
hardcoding numeric tag ids in config (opaque, and ids are venue-internal),
and client-side tag filtering over unfiltered `/events` pages (many wasted
pages; the server-side `tag_id` filter is live-verified).

## D9 — Polymarket gap handling without sequence numbers (M2)

The CLOB market channel has no sequence numbers anywhere — events carry only
a book-state `hash` and a ms-epoch timestamp — so spec §5.1's seq-based
`GapDetected` cannot exist for this venue and the connector never emits it.
Integrity relies on the layered strategy the feed is actually designed for:
the server pushes a *full* book snapshot per token on every (re)subscribe, so
any loss window is healed by the staleness-watchdog reconnect (default 30s,
with client-initiated text `PING` keep-alives every 10s bounding quiet
periods); `price_change` events carry per-asset `best_bid`/`best_ask`, so
top-of-book self-heals on every event rather than depending on cumulative
delta application. Rejected: recomputing Polymarket's book hash locally to
detect divergence (the hashing scheme is undocumented and we don't maintain
full depth in v1 — nothing cheap to compare against), timestamp-monotonicity
pseudo-gap detection (timestamps are per-market wall clocks, not gapless),
and periodic REST `/book` polling for reconciliation (redundant while every
event already carries authoritative top-of-book).

## D10 — Polymarket YES-token mapping: condition id identity, yes-leg per market (M2)

A Polymarket market is one `conditionId` with two outcome tokens, each with
its own book and WSS stream; venuewire's canonical `Tick.yes_*` needs one
probability per market. Convention: the instrument is the *condition id*
(`polymarket:0x…`), and the "yes-leg" is the token whose outcome label is
`"Yes"` (case-insensitive) — for non-Yes/No markets (sports teams, candidate
names) it is outcome index 0, matching Gamma's parallel
`clobTokenIds`/`outcomes` ordering, so "yes" reads as "first-listed outcome
occurs". The connector subscribes only yes-leg tokens (halves the
subscription count; complementarity `p_yes ≈ 1 − p_no` is live-verified), but
the normalizer still orients any NO-leg frame it encounters (`yes_bid = 1 −
no_ask`, `yes_ask = 1 − no_bid`), since `price_change` events and fixtures
can carry both legs. Token→(condition, leg) mapping is learned from the Gamma
discovery frames that always precede WSS frames — in live streams and
recorded fixtures alike — which is why Polymarket normalization is a stateful
`Normalizer` rather than Kalshi's pure function. Rejected: token ids as
instrument ids (would split one market into two instruments and break
cross-venue matching), and subscribing both legs (doubles subscription volume
for information the complement already provides at top-of-book).

## D11 — Rule pass is conservative by construction: anchors, ambiguity, close-time guards (M3)

The rule pass emits `Exact` only on a full normalized-key collision, and the
key itself is gated: it requires at least one numeric/date anchor (a
canonicalized threshold like `pct:4.25` / `num:150000` / `usd:4.5`, or a month
anchor like `2026-07`) plus at least two content tokens, so low-signal titles
("Yes", "Above 4.25%") can never key at all. Two further guards suppress
plausible-looking collisions: a key shared by more than one instrument of the
*same* venue is ambiguous and emits nothing, and legs whose close times (when
both known) disagree by more than 72h are refused even on identical titles.
Different strikes can never collide because the threshold is part of the key,
and the similarity pass additionally drops any pair whose threshold or date
anchor sets are disjoint — the canonical near-miss (same event, adjacent
strike) is excluded from automatic matching entirely. Rejected: fuzzy key
matching and "any shared anchor" keys — both trade false `Exact` matches for
recall, and a false `Exact` feeds garbage straight into divergence detection,
while a missed match merely waits for the similarity pass or a manual entry.
Zero false `Exact` beats recall (spec M3 acceptance).

## D12 — Lexical similarity promotes to `Review` at most; `High` requires LLM adjudication (M3)

The similarity pass shortlists cross-venue pairs closing within ±48h of each
other (missing close times stay candidates with a 0.85 score penalty) by token
Jaccard over normalized title + description. Without LLM adjudication, a score
above the review threshold produces `Review` — never `High` — because token
overlap measures "talks about the same thing", not "resolves identically":
settlement source, tie-breaks, and inclusive/exclusive thresholds live in
prose that set-overlap cannot judge. When `matcher.llm_adjudication` is enabled
*and* `ANTHROPIC_API_KEY` is set, shortlisted pairs are batch-adjudicated via
the Anthropic Messages API (`claude-haiku-4-5`, blocking reqwest run off the
hot path via `spawn_blocking`); a `same: true` verdict at confidence ≥ 0.9
becomes `High` (auto-active), 0.6–0.9 becomes `Review`. An API failure degrades
to lexical-only for that batch rather than failing the pass. The core
`MatchMethod::Embedding` variant labels all similarity-pass matches; the
registry `note` records which ("lexical score 0.62" vs "llm 0.95: …").
Rejected: shipping an ONNX sentence-transformer in v1 (heavy dependency for
marginal gain at this universe size) and letting lexical scores reach `High`.

## D13 — `config/matches.yaml` is the source of truth, with manual-override and rejection semantics (M3)

All matches persist in one human-editable YAML registry that the engine
reloads at the start of every pass and rewrites atomically (temp file + rename
in the same directory, so a crash mid-write never leaves a torn file and human
edits made between passes are always honored). Merge semantics: entries with
`method: manual` are never modified by the engine and their legs (when active)
are withheld from further matching; `status: rejected` entries are never
re-emitted (humans *edit* status to rejected rather than deleting, since
deletion would let the engine re-derive the match); engine findings land as
`active` (Exact/High) or `review` (Review), and an existing `review` entry is
promoted in place — keeping its stable `match_id` — when a later pass produces
a stronger confidence. Rejected: a database table (kills the human-in-the-loop
edit flow that `Review` triage depends on; YAML diffs in PRs are the audit log)
and splitting engine state from human state across two files (moves merge
conflicts into code).

## D14 — DashMap book with a fully synchronous hot path (M4)

`BookState` is a `DashMap<InstrumentId, LatestQuote>` and `PipelineState::apply`
is a plain synchronous function: per tick it does one clock read, one sharded
entry upsert, an optional dirty-marker insert, and (only for matched
instruments) a brief `RwLock` read of the match index plus a per-match view
recompute. Nothing on this path awaits, so no lock can be held across an await
point by construction, and contention is limited to a DashMap shard.
Out-of-order protection is seq-based: `Tick::seq` is connector-local monotonic,
so per instrument a tick with `seq <= stored seq` is reordered or duplicate and
is rejected (counted), keeping the book last-writer-wins in *sequence* order
rather than arrival order. This assumes connectors keep seq monotonic across
reconnects within a process; a daemon restart starts from an empty book, so a
seq reset cannot permanently wedge an instrument.

## D15 — Write-behind Redis mirror that drops on outage (M4)

The hot path never serializes or performs I/O for Redis: it inserts a cheap
payload into a dirty map keyed by the final Redis key (so bursts on one
instrument coalesce to the latest value between flushes). A spawned task drains
the map every 250ms, serializes to JSON, and issues one pipelined batch of
`SET key json EX 3600`. Because Redis is a mirror and not the source of truth,
a failed batch is **dropped**, not retried or queued: retrying would either
grow an unbounded buffer during a long outage or push stale values on recovery,
and the next tick touching an instrument re-marks it dirty anyway (quiet
instruments age out via the 1h TTL). Outage transitions are logged exactly once
per direction and failed flushes counted for `vw_redis_write_errors_total`. The
sink sits behind a `QuoteSink` trait, so batching, TTL, coalescing, outage, and
recovery are all tested with in-memory/failing sinks — no Redis or Docker
needed; a live round-trip test exists but is `#[ignore]`d.

## D16 — Divergence debounce keyed to the last *emitted* spread (M4)

The detector stores, per match, the timestamp and spread of the last emission.
A new threshold crossing within `debounce_secs` is suppressed unless
`spread - last_emitted_spread >= 0.01` (spec §7.2); a widening emission restarts
the window. Comparing against the last *emitted* spread (not the last observed
one) means a slow creep of +0.002 per tick cannot ratchet past the debounce,
while a genuine 1-cent widening always gets through immediately. Spreads that
dip below the threshold neither emit nor clear the record, so flapping around
the threshold still yields at most one event per window unless the divergence
genuinely worsens. Freshness is strict against the leg's `recv_ts` (age < 60s);
for >2-leg matches the spread is the max pairwise mid difference over fresh legs
only, and the event reports all fresh legs. Time is injected (`Clock` trait) so
the matrix is tested deterministically.

## D17 — Broadcast fan-out with lagged-continue semantics (M5)

The server owns a single `tokio::sync::broadcast` channel of `StreamEvent`
(Tick/Divergence, defined in `vw-server` so the serving layer never depends on
`vw-connectors`); the daemon publishes through a `PublishHandle` and every WS
session holds its own receiver plus a local topic set, filtering server-side
before each send. `broadcast::send` never blocks and never fails, so a slow
client cannot backpressure ingest by construction — instead its receiver
overruns the ring buffer, gets `RecvError::Lagged(n)`, and the session sends one
`{"op":"lagged","missed":n}` frame, counts `n` into `vw_ws_lagged_total`, and
**continues** from the oldest retained event. The alternative — per-client
bounded queues with disconnect-on-full — punishes a briefly slow client far
more than losing `n` self-healing top-of-book ticks does (REST snapshots exist
for resync). The Prometheus registry is server-owned and public so M6 registers
pipeline and sink metrics onto the same `/metrics` page.

## D18 — JSONEachRow over HTTP instead of the `clickhouse` crate (M5)

The sink speaks ClickHouse's HTTP interface directly with reqwest 0.12 (already
in tree): `POST /?query=INSERT INTO ticks FORMAT JSONEachRow` with
newline-delimited JSON rows as the body. Versus the `clickhouse` crate's
RowBinary this costs some encode CPU at our volumes (≤ ~10k rows/batch) but wins
on every axis v1 cares about: no new heavyweight dependency, human-readable
payloads (a failed batch replays with `curl`), ClickHouse's actual
`DB::Exception` text surfaces in the HTTP body (captured in
`TransportError::Status`), and the wire format is trivial to fake in tests — the
batching writer is tested against a mock `Transport` plus a local axum stub,
no Docker anywhere. Numeric fidelity is preserved by sending `Decimal(9,6)`
columns as strings (ClickHouse parses decimals from strings natively) and
`DateTime64(3,'UTC')` as `YYYY-MM-DD hh:mm:ss.mmm` text.

## D19 — Sink outage policy: bounded buffer, drop-oldest-batch, counted (M5)

A ClickHouse outage must never stall ingest, so the sink task always keeps
draining its mpsc: rows are encoded on arrival, sealed into batches (500ms or
5_000 rows, whichever first), and failed batches are parked in a FIFO retry
queue retried in order on every subsequent flush — a short blip loses nothing.
The total buffered row count is capped (`max_buffer_rows`, default 100_000 ≈ 20
batches); beyond it the **oldest batch** is dropped and counted in
`vw_clickhouse_rows_dropped_total`. Drop-oldest (vs drop-newest or block) is the
right bias for time-series data: after a long outage the freshest rows are the
ones queries care about, and the recorder's NDJSON fixtures remain the durable
raw record for backfill. Counters and the `vw_clickhouse_batch_flush_seconds`
histogram are plain `prometheus` primitives created unconditionally and
registered on a caller-supplied registry when set, which is how M6 bridges them
onto the server's registry.

## D20 — Metrics bridge: connectors stay Prometheus-free, the daemon owns the registry (M6)

The `/metrics` page unifies four sources onto the server-owned
`prometheus::Registry`: the server (`vw_ws_*`), the sink (`vw_clickhouse_*`, via
`SinkOptions::registry`), the daemon inline (`vw_ingest_to_publish_seconds`,
`vw_ticks_published_total{venue}`, `vw_divergences_total`, updated on the hot
path), and the connectors/mirror (`vw_frames_received_total`,
`vw_reconnects_total`, `vw_gaps_detected_total`, `vw_conflation_events_total`,
`vw_redis_write_errors_total`). The connectors expose only a plain-atomic
`ConnectorMetrics` incremented inside `ConflatingSender` — the single seam every
connector already routes events through, so no venue-specific session code
touches metrics — and the daemon bridges those atomics into registered
`IntCounter`s by delta on a 1s poll. This keeps `vw-connectors` and `vw-state`
free of a Prometheus dependency (they stay usable as plain libraries) and keeps
all registry ownership in the binary. `vw_ingest_to_publish_seconds` is the one
metric measured inline on the hot path because a 1s-lagged latency histogram
would be useless; counters tolerate the lag. Rejected: a shared metrics crate
every component registers into (couples every library to Prometheus and to a
global registry) and a custom `Collector` reading the atomics at scrape time
(more boilerplate than the delta poll for no observable benefit at a 15s scrape
interval).

## D21 — Benchmarks isolate the engine; loopback re-stamps `recv_ts` (M6)

`vwbench` (`make bench`) replays committed fixtures through the real
normalization/state/serving code and reports three things. **Pipeline latency**
times `apply` + broadcast per tick — venuewire's own processing cost — and
deliberately excludes venue-network/socket time, which replay cannot reproduce;
the production `vw_ingest_to_publish_seconds` histogram captures the real
recv→publish figure. **Loopback latency** runs a real axum `/ws` server with 1
and 50 clients and measures ingest→client-receive by re-stamping each tick's
`recv_ts` to now at publish, so the client can compute `now − recv_ts` from the
ordinary wire payload with no side channel and no clock skew (single process).
**Throughput** replays at max speed with the sink path off and on. Fixtures are
looped to a ~50k-tick workload for stable percentiles. Rejected: criterion
(built for micro-benchmarks, not end-to-end latency distributions across an
async server) and requiring a live ClickHouse/Redis (the bench must run
offline; the sink path is modeled by a drained channel).
