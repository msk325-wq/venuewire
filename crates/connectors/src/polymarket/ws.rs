//! Polymarket CLOB WebSocket session: unauthenticated connect, market-channel
//! subscribe by token ids, and the frame read loop.
//!
//! One call to [`run_session`] = one connection lifetime; the caller
//! supervises reconnects/backoff. Peculiarities per
//! docs/research/polymarket-api.md §2:
//! - Subscribe immediately after connect (`{"assets_ids": […], "type":
//!   "market"}`); the server closes connections that never subscribe.
//! - Keep-alive is a *literal text* `PING` the **client** sends every ~10s;
//!   the server replies with a literal text `PONG` (not WebSocket protocol
//!   pings). `PONG` counts as liveness and is skipped before JSON parsing.
//! - A full book snapshot per subscribed asset is pushed on subscribe, so the
//!   WSS session seeds its own book state.
//! - Payloads may arrive as a single JSON object or a JSON array of events;
//!   array batches are split into individual raw frames.

use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use vw_core::{RawFrame, Venue};

use super::normalize::Normalizer;
use crate::conflate::ChannelClosed;
use crate::{Backoff, ConflatingSender, ConnectorEvent, ConnectorStatus, Watchdog};

/// Client-initiated keep-alive interval (research §2: every ~10s).
const PING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(super) enum SessionError {
    /// Downstream receiver dropped; the pipeline is shutting down.
    Closed,
    /// Any connect failure (DNS, TLS, HTTP status, ...).
    Connect(anyhow::Error),
}

impl From<ChannelClosed> for SessionError {
    fn from(_: ChannelClosed) -> Self {
        SessionError::Closed
    }
}

/// Run one WSS session subscribed to the market channel for `token_ids`
/// (yes-leg tokens; D10). Emits `Status(Connected)` after the handshake and
/// resets `backoff`; returns the disconnect reason once the session ends
/// (staleness, server close, stream error). Connect failures return `Err`.
#[allow(clippy::too_many_arguments)] // internal single-caller fn; a params struct would only add ceremony
pub(super) async fn run_session(
    ws_url: &str,
    token_ids: &[String],
    staleness: Duration,
    seq: &mut u64,
    sender: &mut ConflatingSender,
    tap: &Option<mpsc::Sender<RawFrame>>,
    normalizer: &mut Normalizer,
    backoff: &mut Backoff,
) -> Result<String, SessionError> {
    let (mut ws, _resp) = connect_async(ws_url)
        .await
        .map_err(|e| SessionError::Connect(anyhow::anyhow!("WS connect: {e}")))?;

    // Subscribe first thing: the server may close connections that don't
    // subscribe within a timeout.
    let cmd = json!({ "assets_ids": token_ids, "type": "market" });
    if let Err(e) = ws.send(Message::Text(cmd.to_string())).await {
        return Ok(format!("subscribe send failed: {e}"));
    }

    tracing::info!(
        url = ws_url,
        tokens = token_ids.len(),
        "polymarket WS connected"
    );
    backoff.reset();
    sender.try_send(ConnectorEvent::Status(ConnectorStatus::Connected))?;

    let watchdog = Watchdog::new();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await; // first tick fires immediately; skip it

    loop {
        tokio::select! {
            frame = ws.next() => {
                // recv_ts before any parsing (spec §4).
                let recv_ts = Utc::now();
                watchdog.touch();
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        // Literal text PONG keep-alive: liveness only.
                        if text == "PONG" {
                            continue;
                        }
                        let raw: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(error = %e, "polymarket WS sent non-JSON text frame");
                                continue;
                            }
                        };
                        // Array batches split into individual frames so the
                        // recorder and normalizer see uniform events.
                        let events = match raw {
                            Value::Array(events) => events,
                            single => vec![single],
                        };
                        for raw_event in events {
                            let frame = RawFrame { recv_ts, venue: Venue::Polymarket, raw_frame: raw_event };
                            if let Some(tap) = tap {
                                // Recording must never backpressure ingest: drop on full.
                                if tap.try_send(frame.clone()).is_err() {
                                    tracing::debug!("raw-frame tap full or closed; frame not recorded");
                                }
                            }
                            for event in normalizer.normalize_frame(&frame, seq) {
                                sender.try_send(event)?;
                            }
                        }
                    }
                    Some(Ok(Message::Close(close))) => {
                        return Ok(format!("server closed connection: {close:?}"));
                    }
                    // Protocol pings (if any) are answered by tungstenite;
                    // any frame counts as liveness (touched above).
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Ok(format!("WS stream error: {e}")),
                    None => return Ok("WS stream ended".to_string()),
                }
            }
            _ = ping.tick() => {
                if let Err(e) = ws.send(Message::Text("PING".to_string())).await {
                    return Ok(format!("keep-alive PING send failed: {e}"));
                }
            }
            _ = watchdog.wait_stale(staleness) => {
                return Ok(format!("stale: no frame for {}s", staleness.as_secs()));
            }
        }
    }
}
