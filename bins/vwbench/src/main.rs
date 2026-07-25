//! venuewire benchmark harness (spec §9).
//!
//! Replays committed NDJSON fixtures through the real pipeline and reports:
//! 1. **Pipeline latency** — per-tick `PipelineState::apply` + broadcast publish
//!    processing time (p50/p95/p99). This is venuewire's own contribution;
//!    venue network time is excluded (replay can't reproduce it).
//! 2. **Loopback latency** — ingest→client-receive over a real axum `/ws`
//!    server with 1 and with 50 subscribed clients, measured by stamping each
//!    tick's `recv_ts` fresh at publish and computing `now − recv_ts` at the
//!    client (p50/p95/p99).
//! 3. **Throughput** — max-speed replay through the full pipeline with the
//!    ClickHouse sink path off and on (sustained ticks/sec).
//!
//! Reproducible via `make bench`. Deterministic inputs (committed fixtures),
//! no network, no credentials. Numbers land in `docs/benchmarks.md`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use vw_connectors::{ConnectorEvent, ReplayConnector, ReplaySpeed, VenueConnector};
use vw_core::config::DivergenceConfig;
use vw_core::Tick;
use vw_server::{Server, ServerOptions, StreamEvent};
use vw_sink_clickhouse::SinkEvent;
use vw_state::{PipelineState, SystemClock};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let fixtures: Vec<PathBuf> = {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if args.is_empty() {
            vec![
                PathBuf::from("fixtures/committed/polymarket-ws-sample.ndjson"),
                PathBuf::from("fixtures/committed/kalshi-rest-sample.ndjson"),
            ]
        } else {
            args.into_iter().map(PathBuf::from).collect()
        }
    };

    println!("venuewire benchmark harness");
    println!(
        "host: {} cores, rustc replay-driven (no network, no credentials)\n",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );

    // Pool ticks from all fixtures into one workload so percentiles are stable.
    let mut ticks = Vec::new();
    for fixture in &fixtures {
        let loaded = load_ticks(fixture).await?;
        println!(
            "loaded {:>5} ticks from {}",
            loaded.len(),
            fixture.display()
        );
        ticks.extend(loaded);
    }
    anyhow::ensure!(!ticks.is_empty(), "no ticks loaded from fixtures");
    // Loop the workload so throughput/latency runs have enough samples.
    let target = 50_000usize;
    let reps = target.div_ceil(ticks.len()).max(1);
    let workload: Vec<Tick> = ticks
        .iter()
        .cloned()
        .cycle()
        .take(ticks.len() * reps)
        .collect();
    println!(
        "\nworkload: {} ticks ({} fixture ticks × {} reps)\n",
        workload.len(),
        ticks.len(),
        reps
    );

    pipeline_latency(&workload);
    throughput(&workload, false);
    throughput(&workload, true);
    loopback(&ticks, 1).await?;
    loopback(&ticks, 50).await?;

    println!("\ndone.");
    Ok(())
}

/// Replay a fixture through the real replay connector + normalization, collecting
/// the normalized ticks.
async fn load_ticks(fixture: &Path) -> anyhow::Result<Vec<Tick>> {
    anyhow::ensure!(
        fixture.exists(),
        "fixture not found: {} (run from the repo root)",
        fixture.display()
    );
    let (tx, mut rx) = mpsc::channel::<ConnectorEvent>(16_384);
    let connector = ReplayConnector::new(fixture.to_path_buf(), ReplaySpeed::Max);
    let task = tokio::spawn(connector.run(tx));
    let mut ticks = Vec::new();
    while let Some(event) = rx.recv().await {
        if let ConnectorEvent::Tick(t) = event {
            ticks.push(t);
        }
    }
    let _ = task.await;
    Ok(ticks)
}

/// Per-tick `apply` + publish processing latency.
fn pipeline_latency(workload: &[Tick]) {
    let state = PipelineState::new(
        DivergenceConfig::default(),
        &[],
        Arc::new(SystemClock),
        None,
    );
    let (tx, _rx) = tokio::sync::broadcast::channel::<StreamEvent>(1 << 16);
    let mut samples = Vec::with_capacity(workload.len());
    for tick in workload {
        let t0 = Instant::now();
        let _ = state.apply(tick);
        let _ = tx.send(StreamEvent::Tick(tick.clone()));
        samples.push(t0.elapsed().as_secs_f64() * 1e6); // microseconds
    }
    report_latency(
        "pipeline ingest→publish (apply + broadcast)",
        &samples,
        "µs",
    );
}

/// Max-speed throughput through the pipeline, optionally exercising the sink path.
fn throughput(workload: &[Tick], sink_on: bool) {
    let state = PipelineState::new(
        DivergenceConfig::default(),
        &[],
        Arc::new(SystemClock),
        None,
    );
    let (bcast, _rx) = tokio::sync::broadcast::channel::<StreamEvent>(1 << 16);
    // A bounded sink channel drained by a background thread models the daemon's
    // try_send-to-ClickHouse cost without needing a live ClickHouse.
    let sink_tx = if sink_on {
        let (stx, mut srx) = mpsc::channel::<SinkEvent>(1 << 16);
        std::thread::spawn(move || while srx.blocking_recv().is_some() {});
        Some(stx)
    } else {
        None
    };

    let start = Instant::now();
    for tick in workload {
        let _ = state.apply(tick);
        let _ = bcast.send(StreamEvent::Tick(tick.clone()));
        if let Some(stx) = &sink_tx {
            let _ = stx.try_send(SinkEvent::Tick(tick.clone()));
        }
    }
    let elapsed = start.elapsed();
    let rate = workload.len() as f64 / elapsed.as_secs_f64();
    println!(
        "throughput  (sink {:>3}): {:>10.0} ticks/sec  ({} ticks in {:.3}s)",
        if sink_on { "on" } else { "off" },
        rate,
        workload.len(),
        elapsed.as_secs_f64()
    );
}

/// Ingest→client-receive latency over a real WS server with `clients` subscribers.
async fn loopback(ticks: &[Tick], clients: usize) -> anyhow::Result<()> {
    let state = Arc::new(PipelineState::new(
        DivergenceConfig::default(),
        &[],
        Arc::new(SystemClock),
        None,
    ));
    let server = Server::new(Arc::clone(&state), ServerOptions::default());
    let publish = server.publish.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding loopback bench server")?;
    let addr = listener.local_addr()?;
    let server_task = tokio::spawn(async move {
        let _ = axum::serve(listener, server.router).await;
    });

    // One instrumented monitor client + (clients − 1) load clients.
    let (lat_tx, mut lat_rx) = mpsc::unbounded_channel::<f64>();
    let url = format!("ws://{addr}/ws");
    let mut client_tasks = Vec::new();
    for i in 0..clients {
        let url = url.clone();
        let lat_tx = (i == 0).then(|| lat_tx.clone());
        client_tasks.push(tokio::spawn(ws_client(url, lat_tx)));
    }
    drop(lat_tx);
    // Give clients a moment to connect + subscribe.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish each tick with a fresh recv_ts, paced to avoid self-inflicted queueing.
    let sample = ticks.iter().cycle().take(3_000);
    for tick in sample {
        let mut t = tick.clone();
        t.recv_ts = Utc::now();
        let _ = state.apply(&t);
        publish.publish_tick(t);
        tokio::time::sleep(Duration::from_micros(200)).await;
    }
    // Let the last frames drain, then tear down.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for c in &client_tasks {
        c.abort();
    }
    server_task.abort();

    let mut samples = Vec::new();
    while let Ok(us) = lat_rx.try_recv() {
        samples.push(us);
    }
    report_latency(
        &format!(
            "loopback ingest→client-receive ({clients} client{})",
            plural(clients)
        ),
        &samples,
        "µs",
    );
    Ok(())
}

/// A WS client: subscribe to all ticks; if `lat_tx` is set, report each tick's
/// `now − recv_ts` loopback latency.
async fn ws_client(url: String, lat_tx: Option<mpsc::UnboundedSender<f64>>) -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::Message;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await?;
    ws.send(Message::Text(
        serde_json::json!({"op":"subscribe","topics":["ticks:*"]}).to_string(),
    ))
    .await?;
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Text(text) = msg {
            let Some(tx) = &lat_tx else { continue };
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                if v.get("op").and_then(|o| o.as_str()) == Some("tick") {
                    if let Some(recv) = v
                        .get("data")
                        .and_then(|d| d.get("recv_ts"))
                        .and_then(|r| r.as_str())
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    {
                        let us = (Utc::now() - recv.with_timezone(&Utc))
                            .num_microseconds()
                            .unwrap_or(0)
                            .max(0) as f64;
                        let _ = tx.send(us);
                    }
                }
            }
        }
    }
    Ok(())
}

fn report_latency(label: &str, samples: &[f64], unit: &str) {
    if samples.is_empty() {
        println!("{label}: no samples");
        return;
    }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = s.iter().sum::<f64>() / s.len() as f64;
    println!(
        "{label}:\n  n={} mean={:.1}{unit}  p50={:.1}{unit}  p95={:.1}{unit}  \
         p99={:.1}{unit}  max={:.1}{unit}",
        s.len(),
        mean,
        pct(&s, 50.0),
        pct(&s, 95.0),
        pct(&s, 99.0),
        s[s.len() - 1],
    );
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn plural(n: usize) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}
