//! venuewire daemon: wires connectors → state → sinks → serving layer.
//!
//! Connectors (Kalshi/Polymarket, per `[venue].enabled`) feed one bounded
//! event channel. The consumer applies every tick to [`PipelineState`] (book
//! state + match views + divergence detection), publishes ticks and divergence
//! events to the WS fan-out, and forwards both to the ClickHouse sink. A
//! matcher task refreshes the cross-venue match set off the hot path; an
//! optional Redis mirror write-behinds hot state. The axum server exposes REST
//! snapshots, `/ws`, and `/metrics`.
//!
//! `--record` attaches one recorder per venue so raw frames land in NDJSON
//! fixtures. `--config <path>` (or `VW_CONFIG`) selects the config file.

mod metrics;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tokio::sync::mpsc;
use vw_connectors::{
    ConnectorEvent, ConnectorMetrics, ConnectorStatus, KalshiConnector, PolymarketConnector,
    VenueConnector,
};
use vw_core::{Config, Instrument, InstrumentId, Venue};
use vw_matcher::{Matcher, MatcherOptions};
use vw_recorder::Recorder;
use vw_server::{PublishHandle, Server, ServerOptions};
use vw_sink_clickhouse::{ClickHouseSink, SinkEvent, SinkOptions};
use vw_state::{
    Clock, MirrorHandle, PipelineState, RedisSink, SystemClock, DEFAULT_FLUSH_INTERVAL,
};

use metrics::{spawn_bridge, DaemonMetrics, VenueSource};

/// The cross-venue match registry (spec §6). Human-editable; the matcher
/// reloads and rewrites it every pass.
const MATCHES_PATH: &str = "config/matches.yaml";

struct Args {
    config: PathBuf,
    record: bool,
}

fn parse_args() -> Args {
    // `--config <path>` beats VW_CONFIG beats ./config/default.toml
    let mut config = None;
    let mut record = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = args.next().map(PathBuf::from),
            "--record" => record = true,
            other => {
                eprintln!(
                    "venuewire: unknown argument {other:?} (supported: --config <path>, --record)"
                );
                std::process::exit(2);
            }
        }
    }
    let config = config
        .or_else(|| std::env::var_os("VW_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("config/default.toml"));
    Args { config, record }
}

/// Instruments discovered so far, shared between the ingest consumer (writer)
/// and the matcher task (reader). Locked only for brief insert/snapshot; the
/// matcher clones values out and releases before running a pass.
type InstrumentBook = Arc<Mutex<HashMap<InstrumentId, Instrument>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let json_logs = std::env::var("VW_LOG_FORMAT").is_ok_and(|v| v == "json");
    vw_core::telemetry::init_tracing(json_logs);

    let args = parse_args();
    let config = Config::load(&args.config)
        .with_context(|| format!("loading config from {}", args.config.display()))?;

    tracing::info!(
        config = %args.config.display(),
        kalshi = config.kalshi.enabled,
        polymarket = config.polymarket.enabled,
        server_bind = %config.server.bind,
        record = args.record,
        "venuewire starting"
    );

    // --- Redis mirror (optional; a missing/unavailable Redis is non-fatal) ---
    let mirror = match RedisSink::connect(&config.redis.url).await {
        Ok(sink) => {
            let handle = MirrorHandle::new();
            let task = handle.spawn(sink, DEFAULT_FLUSH_INTERVAL);
            tracing::info!(url = %config.redis.url, "redis mirror connected");
            Some((handle, task))
        }
        Err(e) => {
            tracing::warn!(url = %config.redis.url, error = %e,
                "redis unavailable; running without hot-state mirror");
            None
        }
    };
    let mirror_handle = mirror.as_ref().map(|(h, _)| h.clone());

    // --- Matcher: seed the initial match set from the persisted registry ---
    let matcher = Matcher::new(
        MatcherOptions {
            llm_adjudication: config.matcher.llm_adjudication,
            ..MatcherOptions::default()
        },
        MATCHES_PATH,
    );
    let initial_matches = matcher.active_matches().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "could not load match registry; starting with no matches");
        Vec::new()
    });
    tracing::info!(
        matches = initial_matches.len(),
        "seeded match set from registry"
    );

    // --- Pipeline state (book + match views + divergence) ---
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let pipeline = Arc::new(PipelineState::new(
        config.divergence.clone(),
        &initial_matches,
        clock,
        mirror_handle,
    ));

    // --- Server (REST + WS + /metrics); its registry hosts sink metrics too ---
    let server = Server::new(
        Arc::clone(&pipeline),
        ServerOptions::from_config(&config.server),
    );
    let publish = server.publish.clone();
    let registry = server.metrics.registry();
    let router = server.router;
    let daemon_metrics = DaemonMetrics::register(registry).context("registering metrics")?;

    // --- ClickHouse sink (best-effort schema; outages never stall ingest) ---
    let (sink, sink_tx) = ClickHouseSink::new(SinkOptions {
        registry: Some(registry.clone()),
        ..SinkOptions::from_config(&config.clickhouse)
    })
    .context("building clickhouse sink")?;
    match sink.ensure_schema().await {
        Ok(()) => tracing::info!(url = %config.clickhouse.url, "clickhouse schema ready"),
        Err(e) => tracing::warn!(url = %config.clickhouse.url, error = %e,
            "clickhouse schema setup failed; sink will retry/buffer, ingest continues"),
    }
    let sink_task = tokio::spawn(sink.run());

    // --- Connectors into one bounded ingest channel ---
    let (tx, rx) = mpsc::channel::<ConnectorEvent>(config.ingest.channel_capacity);
    let mut connector_tasks = Vec::new();
    let mut recorders = Vec::new();
    let mut metric_sources = Vec::new();

    if config.kalshi.enabled {
        let cm = Arc::new(ConnectorMetrics::new());
        let mut connector = KalshiConnector::from_env(config.kalshi.clone(), &config.ingest)
            .context("configuring kalshi connector")?
            .with_metrics(Arc::clone(&cm));
        if args.record {
            let rec = Recorder::start(Path::new("fixtures"), Venue::Kalshi)
                .context("starting kalshi recorder")?;
            connector = connector.with_raw_tap(rec.tap());
            recorders.push(rec);
        }
        metric_sources.push(VenueSource {
            venue: Venue::Kalshi.as_str(),
            metrics: cm,
        });
        connector_tasks.push(tokio::spawn(connector.run(tx.clone())));
    }
    if config.polymarket.enabled {
        let cm = Arc::new(ConnectorMetrics::new());
        let mut connector = PolymarketConnector::new(config.polymarket.clone(), &config.ingest)
            .with_metrics(Arc::clone(&cm));
        if args.record {
            let rec = Recorder::start(Path::new("fixtures"), Venue::Polymarket)
                .context("starting polymarket recorder")?;
            connector = connector.with_raw_tap(rec.tap());
            recorders.push(rec);
        }
        metric_sources.push(VenueSource {
            venue: Venue::Polymarket.as_str(),
            metrics: cm,
        });
        connector_tasks.push(tokio::spawn(connector.run(tx.clone())));
    }
    if connector_tasks.is_empty() {
        tracing::warn!("all venues disabled in config; no connectors to run");
    }
    drop(tx); // consumer ends when the last connector sender drops

    // Bridge connector + mirror atomics onto the /metrics registry.
    let bridge_task = spawn_bridge(
        registry,
        metric_sources,
        mirror.as_ref().map(|(h, _)| h.clone()),
    )
    .context("registering bridged metrics")?;

    // --- Ingest consumer: the hot path ---
    let instruments: InstrumentBook = Arc::new(Mutex::new(HashMap::new()));
    let consumer = tokio::spawn(consume(
        rx,
        Arc::clone(&pipeline),
        publish,
        sink_tx,
        Arc::clone(&instruments),
        daemon_metrics,
    ));

    // --- Matcher refresh loop (off the hot path) ---
    let matcher_task = tokio::spawn(matcher_loop(
        matcher,
        Arc::clone(&pipeline),
        instruments,
        Duration::from_secs(config.matcher.refresh_secs),
    ));

    // --- Serve ---
    let listener = tokio::net::TcpListener::bind(&config.server.bind)
        .await
        .with_context(|| format!("binding server to {}", config.server.bind))?;
    tracing::info!(bind = %config.server.bind, "serving REST + /ws + /metrics");
    let server_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!(error = %e, "server exited with error");
        }
    });

    tokio::signal::ctrl_c()
        .await
        .context("waiting for shutdown signal")?;
    tracing::info!("shutdown signal received, stopping");

    // Stop producers first, then drain, then flush persistence.
    for task in &connector_tasks {
        task.abort();
    }
    matcher_task.abort();
    server_task.abort();
    bridge_task.abort();
    // Aborting the consumer drops its sink sender, so the sink sees the channel
    // close and performs its final flush; wait briefly for that.
    consumer.abort();
    let _ = tokio::time::timeout(Duration::from_secs(2), sink_task).await;
    if let Some((handle, task)) = mirror {
        handle.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }
    for rec in recorders {
        rec.shutdown().await.context("flushing recorder")?;
    }
    tracing::info!("shutdown complete");
    Ok(())
}

/// Hot path: apply ticks to state, fan out to WS + ClickHouse, track discovery.
async fn consume(
    mut rx: mpsc::Receiver<ConnectorEvent>,
    pipeline: Arc<PipelineState>,
    publish: PublishHandle,
    sink_tx: mpsc::Sender<SinkEvent>,
    instruments: InstrumentBook,
    metrics: DaemonMetrics,
) {
    const REPORT_EVERY: Duration = Duration::from_secs(10);
    let mut ticks_in_window = 0u64;
    let mut total_ticks = 0u64;
    let mut divergences = 0u64;
    let mut sink_drops = 0u64;
    let mut report = tokio::time::interval(REPORT_EVERY);
    report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    report.tick().await; // first tick fires immediately; skip it

    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Some(ConnectorEvent::Tick(tick)) => {
                    ticks_in_window += 1;
                    total_ticks += 1;
                    let events = pipeline.apply(&tick);
                    // ingest → publish latency (recv_ts → now, just before fan-out).
                    let latency = (Utc::now() - tick.recv_ts).num_microseconds();
                    if let Some(us) = latency {
                        metrics.observe_latency(us as f64 / 1_000_000.0);
                    }
                    metrics.tick_published(tick.venue.as_str());
                    // Fan out the tick, then any divergences it triggered.
                    publish.publish_tick(tick.clone());
                    if sink_tx.try_send(SinkEvent::Tick(tick)).is_err() {
                        sink_drops += 1;
                    }
                    for div in events {
                        divergences += 1;
                        metrics.divergence();
                        tracing::info!(
                            match_id = %div.match_id, spread = %div.spread,
                            "divergence detected"
                        );
                        publish.publish_divergence(div.clone());
                        if sink_tx.try_send(SinkEvent::Divergence(div)).is_err() {
                            sink_drops += 1;
                        }
                    }
                }
                Some(ConnectorEvent::Instrument(inst)) => {
                    tracing::debug!(id = %inst.id, title = %inst.title, "instrument discovered");
                    if let Ok(mut book) = instruments.lock() {
                        book.insert(inst.id.clone(), inst);
                    }
                }
                Some(ConnectorEvent::Status(status)) => match status {
                    ConnectorStatus::Connected => tracing::info!("connector status: connected"),
                    ConnectorStatus::Disconnected { reason } => {
                        tracing::warn!(%reason, "connector status: disconnected");
                    }
                    ConnectorStatus::GapDetected { expected, got } => {
                        tracing::warn!(expected, got, "connector status: gap detected");
                    }
                },
                None => {
                    tracing::info!(total_ticks, divergences, "event channel closed; consumer exiting");
                    return;
                }
            },
            _ = report.tick() => {
                let (accepted, rejected) = pipeline.tick_counts();
                tracing::info!(
                    ticks_per_sec = ticks_in_window as f64 / REPORT_EVERY.as_secs_f64(),
                    total_ticks,
                    divergences,
                    instruments = pipeline.instrument_count(),
                    matches = pipeline.match_count(),
                    accepted,
                    rejected_out_of_order = rejected,
                    sink_drops,
                    ws_clients = publish.subscriber_count(),
                    "ingest throughput"
                );
                ticks_in_window = 0;
            }
        }
    }
}

/// Off the hot path: periodically re-run matching over discovered instruments
/// and swap the refreshed match set into the pipeline. The first pass runs
/// after a short delay so venue discovery can populate; `run_pass` (fs I/O and
/// possibly a blocking LLM call) runs on the blocking pool.
async fn matcher_loop(
    mut matcher: Matcher,
    pipeline: Arc<PipelineState>,
    instruments: InstrumentBook,
    refresh: Duration,
) {
    // Let discovery populate before the first pass, but don't wait a full
    // (default 10min) refresh interval for it.
    let initial_delay = refresh.min(Duration::from_secs(10));
    let mut first = true;
    loop {
        tokio::time::sleep(if first { initial_delay } else { refresh }).await;
        first = false;

        let snapshot: Vec<Instrument> = match instruments.lock() {
            Ok(book) => book.values().cloned().collect(),
            Err(_) => continue,
        };
        if snapshot.is_empty() {
            continue;
        }

        // Move the matcher onto the blocking pool for the pass, take it back after.
        let (returned, result) = tokio::task::spawn_blocking(move || {
            let mut m = matcher;
            let outcome = m
                .run_pass(&snapshot)
                .and_then(|report| m.active_matches().map(|matches| (report, matches)));
            (m, outcome)
        })
        .await
        .expect("matcher pass panicked");
        matcher = returned;

        match result {
            Ok((report, matches)) => {
                pipeline.set_matches(&matches);
                tracing::info!(
                    new = report.new_matches,
                    promoted = report.promoted,
                    skipped = report.skipped,
                    active = matches.len(),
                    "match refresh complete"
                );
            }
            Err(e) => tracing::warn!(error = %e, "match pass failed; keeping previous match set"),
        }
    }
}
