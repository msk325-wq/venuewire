# venuewire

Real-time cross-venue event market aggregation engine. Ingests live market
data from Kalshi and Polymarket, normalizes it into a unified schema, matches
equivalent contracts across venues, maintains a best-price view, detects
cross-venue divergence, and serves everything over a low-latency streaming
API — with published latency benchmarks.

Built in Rust to explore production-grade streaming systems engineering: async
ingestion, backpressure, distributed state, and performance measurement.

## Highlights

- **Low-latency hot path** — `~0.2µs` p50 to apply a tick to book state and
  publish it; sustained `~7M ticks/sec` through the full pipeline
  (`~2.2M` with the persistence sink engaged). Loopback ingest→client-receive
  `~77µs` p50 (1 client) / `~380µs` p50 (50 clients). Numbers reproducible via
  `make bench` — see [docs/benchmarks.md](docs/benchmarks.md).
- **Streaming ingestion, done properly** — live WebSocket connectors with
  jittered-backoff reconnect, staleness watchdogs, sequence-gap detection, and
  bounded-channel backpressure via **per-instrument conflation** (never
  drop-oldest).
- **Fan-out serving** — axum REST snapshots + a `tokio::broadcast` WebSocket
  fan-out with topic subscriptions and lagged-client handling that never
  backpressures ingest.
- **Data systems** — **ClickHouse** batching sink (500ms / 5k-row flushes,
  drop-oldest under outage) and a write-behind **Redis** hot-state mirror; both
  degrade gracefully — a downstream outage never stalls ingestion.
- **Observability** — full Prometheus metric set on `/metrics` (ingest→publish
  latency histogram, per-venue throughput, reconnects, conflation, sink health).
- **Deterministic & tested** — record/replay harness runs the whole pipeline
  offline from committed fixtures; **143 tests**, `clippy -D warnings` clean, CI.

**Stack:** Rust · tokio · axum · tokio-tungstenite · ClickHouse · Redis ·
Prometheus · Docker Compose. Cargo workspace, 7 crates + 3 binaries.

**Status: v1 complete (M0–M6).** Both connectors → book state + cross-venue
matching → divergence detection → axum REST/WS/metrics, with Redis and
ClickHouse persistence, the full Prometheus metric set, and reproducible
benchmarks. See [docs/architecture.md](docs/architecture.md) for the design,
[docs/benchmarks.md](docs/benchmarks.md) for latency/throughput numbers, and
[docs/decisions.md](docs/decisions.md) for the running decision log (D1–D21).

## Quick start

```sh
# infra (redis + clickhouse) — optional; the daemon runs without them, logging
# a warning and skipping the mirror/sink rather than failing
docker compose up -d

# checks: fmt + clippy -D warnings + tests
make check

# run the full daemon (connectors + state + divergence + server on :8080).
# loads config/default.toml; override with --config or VW_* env.
# add --record to capture raw frames to fixtures/
cargo run --bin venuewire

# --- demo surface (against a running daemon) ---
# live tick stream over the daemon's WS fan-out
cargo run --bin vwtap -- ticks --connect ws://localhost:8080/ws
# cross-venue divergence events (the headline feature)
cargo run --bin vwtap -- divergence
# REST snapshots
curl localhost:8080/instruments
curl localhost:8080/matches
curl localhost:8080/metrics

# --- standalone taps (no daemon needed) ---
# live Kalshi ticks (REST polling without credentials; set KALSHI_API_KEY_ID +
# KALSHI_PRIVATE_KEY_PATH for the authenticated WSS stream)
cargo run --bin vwtap -- ticks --venue kalshi
# live Polymarket ticks (no credentials needed at all)
cargo run --bin vwtap -- ticks --venue polymarket
# replay a committed fixture — zero network, zero credentials
cargo run --bin vwtap -- ticks --replay fixtures/committed/kalshi-rest-sample.ndjson --speed max
cargo run --bin vwtap -- ticks --replay fixtures/committed/polymarket-ws-sample.ndjson --speed max

# --- benchmarks (replay-driven, offline) ---
make bench   # latency p50/p95/p99 + throughput; see docs/benchmarks.md
```

## Layout

| Path | Purpose |
|---|---|
| `crates/core` | canonical types, config, telemetry |
| `crates/connectors` | Kalshi / Polymarket / replay connectors |
| `crates/matcher` | cross-venue market matching |
| `crates/state` | book state, Redis mirror, divergence detection |
| `crates/sink-clickhouse` | batching tick persistence |
| `crates/server` | axum REST + WS fan-out + `/metrics` |
| `crates/recorder` | live-feed capture to NDJSON fixtures |
| `bins/venuewire` | main daemon |
| `bins/vwtap` | CLI consumer / demo surface |
| `bins/vwbench` | replay-driven latency + throughput benchmarks |
