//! vwtap: CLI consumer that pretty-prints venuewire market data.
//!
//! Two modes:
//! - **in-process** (`vwtap ticks`): runs a connector directly — live Kalshi
//!   (WSS or degraded REST polling per credentials), live Polymarket
//!   (unauthenticated CLOB WSS), or the replay connector over a recorded
//!   fixture. Works offline for demos and fixtures, no daemon required.
//! - **WS client** (`vwtap ticks --connect ws://…`, `vwtap divergence`):
//!   subscribes to a running daemon's `/ws` endpoint and prints the fan-out
//!   stream. This is the demo surface for cross-venue divergence.

use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use owo_colors::OwoColorize;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;
use vw_connectors::{
    ConnectorEvent, ConnectorStatus, KalshiConnector, PolymarketConnector, ReplayConnector,
    ReplaySpeed, VenueConnector,
};
use vw_core::{Config, DivergenceEvent, Tick};

#[derive(Debug, Parser)]
#[command(name = "vwtap", about = "Pretty-print venuewire market data", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Stream ticks: in-process from a connector/fixture, or from a running
    /// daemon's `/ws` when `--connect` is given.
    Ticks {
        /// Venue to stream (`kalshi` or `polymarket`). In-process: which
        /// connector to run (default `kalshi`). With `--connect`: filter to
        /// this venue (omit for all venues).
        #[arg(long)]
        venue: Option<String>,
        /// Subscribe to a running daemon's WS endpoint instead of running a
        /// connector in-process, e.g. `ws://localhost:8080/ws`.
        #[arg(long, value_name = "WS_URL")]
        connect: Option<String>,
        /// Replay this NDJSON fixture instead of connecting live (in-process).
        #[arg(long, value_name = "FIXTURE", conflicts_with = "connect")]
        replay: Option<PathBuf>,
        /// Replay pacing: a real-time multiplier (e.g. 2.5) or `max`.
        #[arg(long, default_value = "1", value_name = "N|max")]
        speed: ReplaySpeed,
        /// Config file (defaults to config/default.toml, then VW_* env).
        #[arg(long, value_name = "PATH", default_value = "config/default.toml")]
        config: PathBuf,
    },
    /// Stream cross-venue divergence events from a running daemon's `/ws`.
    Divergence {
        /// Daemon WS endpoint.
        #[arg(long, value_name = "WS_URL", default_value = "ws://localhost:8080/ws")]
        connect: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connector logs go to stderr and stay out of the tick stream; default to
    // warnings only unless RUST_LOG says otherwise.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Ticks {
            venue,
            connect,
            replay,
            speed,
            config,
        } => match connect {
            Some(url) => {
                let topics = match &venue {
                    Some(v) => vec![format!("ticks:{}:*", v.to_lowercase())],
                    None => vec!["ticks:*".to_string()],
                };
                ws_client(&url, topics).await
            }
            None => ticks(venue.as_deref().unwrap_or("kalshi"), replay, speed, &config).await,
        },
        Command::Divergence { connect } => {
            ws_client(&connect, vec!["divergence:*".to_string()]).await
        }
    }
}

/// WS-client mode: connect to a daemon's `/ws`, subscribe to `topics`, and
/// pretty-print the fan-out stream until Ctrl-C.
async fn ws_client(url: &str, topics: Vec<String>) -> anyhow::Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    eprintln!(
        "{} connecting to {} (topics: {})",
        "vwtap:".dimmed(),
        url,
        topics.join(", ")
    );
    let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| format!("connecting to {url}"))?;

    let subscribe = serde_json::json!({ "op": "subscribe", "topics": topics }).to_string();
    ws.send(Message::Text(subscribe))
        .await
        .context("sending subscribe frame")?;

    let mut events = 0u64;
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => events += handle_frame(text.as_str()),
                Some(Ok(Message::Close(_))) | None => {
                    eprintln!("{} connection closed by server", "vwtap:".dimmed());
                    break;
                }
                Some(Ok(_)) => {} // ping/pong/binary — ignore
                Some(Err(e)) => {
                    eprintln!("{} websocket error: {e}", "vwtap:".dimmed());
                    break;
                }
            },
            _ = tokio::signal::ctrl_c() => {
                eprintln!("{} interrupted", "vwtap:".dimmed());
                break;
            }
        }
    }
    eprintln!("{} {} events", "vwtap:".dimmed(), events.bold());
    Ok(())
}

/// Render one server frame; returns 1 if it was a data event (tick/divergence).
fn handle_frame(text: &str) -> u64 {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return 0;
    };
    match v.get("op").and_then(|o| o.as_str()) {
        Some("tick") => match v.get("data").cloned().map(serde_json::from_value::<Tick>) {
            Some(Ok(tick)) => {
                print_tick(&tick);
                1
            }
            _ => 0,
        },
        Some("divergence") => {
            match v
                .get("data")
                .cloned()
                .map(serde_json::from_value::<DivergenceEvent>)
            {
                Some(Ok(div)) => {
                    print_divergence(&div);
                    1
                }
                _ => 0,
            }
        }
        Some("lagged") => {
            let missed = v.get("missed").and_then(|m| m.as_u64()).unwrap_or(0);
            println!("{} {} missed {missed} events", stamp(), "lagged".yellow());
            0
        }
        Some("subscribed") | Some("unsubscribed") => {
            let topics = v.get("topics").map(|t| t.to_string()).unwrap_or_default();
            eprintln!("{} subscribed: {}", "vwtap:".dimmed(), topics.dimmed());
            0
        }
        Some("error") => {
            let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
            eprintln!("{} server error: {msg}", "vwtap:".dimmed());
            0
        }
        _ => 0,
    }
}

async fn ticks(
    venue: &str,
    replay: Option<PathBuf>,
    speed: ReplaySpeed,
    config_path: &Path,
) -> anyhow::Result<()> {
    let config = Config::load(config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;
    let (tx, mut rx) = mpsc::channel::<ConnectorEvent>(config.ingest.channel_capacity);

    let connector_task = match replay {
        Some(fixture) => {
            eprintln!(
                "{} replaying {} at speed {speed:?}",
                "vwtap:".dimmed(),
                fixture.display(),
            );
            tokio::spawn(ReplayConnector::new(fixture, speed).run(tx))
        }
        None if venue.eq_ignore_ascii_case("kalshi") => {
            let connector = KalshiConnector::from_env(config.kalshi.clone(), &config.ingest)?;
            eprintln!(
                "{} connecting to kalshi (ctrl-c to exit)",
                "vwtap:".dimmed()
            );
            tokio::spawn(connector.run(tx))
        }
        None if venue.eq_ignore_ascii_case("polymarket") => {
            let connector = PolymarketConnector::new(config.polymarket.clone(), &config.ingest);
            eprintln!(
                "{} connecting to polymarket (ctrl-c to exit)",
                "vwtap:".dimmed()
            );
            tokio::spawn(connector.run(tx))
        }
        None => anyhow::bail!("unknown venue {venue:?}: expected `kalshi` or `polymarket`"),
    };

    let mut ticks = 0u64;
    let mut instruments = 0u64;
    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(event) => {
                        print_event(&event);
                        match event {
                            ConnectorEvent::Tick(_) => ticks += 1,
                            ConnectorEvent::Instrument(_) => instruments += 1,
                            ConnectorEvent::Status(_) => {}
                        }
                    }
                    // Connector finished (replay EOF) and dropped the sender.
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("{} interrupted", "vwtap:".dimmed());
                break;
            }
        }
    }
    connector_task.abort();
    eprintln!(
        "{} {} ticks, {} instruments",
        "vwtap:".dimmed(),
        ticks.bold(),
        instruments
    );
    Ok(())
}

fn print_event(event: &ConnectorEvent) {
    match event {
        ConnectorEvent::Tick(tick) => print_tick(tick),
        ConnectorEvent::Instrument(inst) => {
            println!(
                "{} {} {} {}",
                stamp(),
                inst.venue.to_string().magenta(),
                "inst".dimmed(),
                format!("{} — {}", native_id(&inst.id.0), inst.title).dimmed()
            );
        }
        ConnectorEvent::Status(status) => {
            let line = match status {
                ConnectorStatus::Connected => "connected".green().to_string(),
                ConnectorStatus::Disconnected { reason } => {
                    format!("{} ({reason})", "disconnected".red())
                }
                ConnectorStatus::GapDetected { expected, got } => {
                    format!("{} expected seq {expected}, got {got}", "gap".yellow())
                }
            };
            println!("{} {} {line}", stamp(), "status".yellow());
        }
    }
}

fn print_tick(tick: &Tick) {
    let age_ms = (Utc::now() - tick.recv_ts).num_milliseconds().max(0);
    let age = if age_ms < 1_000 {
        format!("{age_ms}ms")
    } else {
        format!("{:.1}s", age_ms as f64 / 1000.0)
    };
    println!(
        "{} {} {:<28} bid {} ask {} last {} {}",
        stamp(),
        tick.venue.to_string().magenta(),
        native_id(&tick.instrument.0).cyan().bold(),
        fmt_price(tick.yes_bid).green(),
        fmt_price(tick.yes_ask).red(),
        fmt_price(tick.last_price).bold(),
        format!("age {age}").dimmed(),
    );
}

/// Screenshot-worthy divergence line: match id, each leg's venue + mid, spread.
fn print_divergence(div: &DivergenceEvent) {
    let legs: Vec<String> = div
        .legs
        .iter()
        .map(|(id, mid)| {
            let (venue, native) = id.0.split_once(':').unwrap_or(("?", id.0.as_str()));
            format!(
                "{}:{} @ {}",
                venue.magenta(),
                native,
                fmt_price(Some(*mid)).bold()
            )
        })
        .collect();
    println!(
        "{} {} {}  {}  spread {}",
        stamp(),
        " DIVERGENCE ".on_red().white().bold(),
        div.match_id.cyan().bold(),
        legs.join("  vs  "),
        format!("{}", div.spread).red().bold(),
    );
}

fn stamp() -> String {
    Utc::now()
        .format("%H:%M:%S%.3f")
        .to_string()
        .dimmed()
        .to_string()
}

/// Strip the `venue:` prefix for display; the venue is printed separately.
fn native_id(id: &str) -> &str {
    id.split_once(':').map_or(id, |(_, native)| native)
}

fn fmt_price(p: Option<Decimal>) -> String {
    match p {
        Some(p) => format!("{p:>6}"),
        None => format!("{:>6}", "—"),
    }
}
