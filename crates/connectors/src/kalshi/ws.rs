//! Kalshi WebSocket session: authenticated connect, ticker-channel subscribe,
//! and the frame read loop.
//!
//! One call to [`run_session`] = one connection lifetime. The caller supervises
//! reconnects/backoff. Keep-alive: Kalshi sends protocol-level pings every 10s
//! (docs/research/kalshi-api.md §2); tungstenite 0.24 queues pong replies
//! automatically and flushes them while the stream is polled for reading
//! (verified in tungstenite-0.24.0 `protocol/mod.rs`), so no manual pong
//! handling is needed — the read loop below is always polling.

use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use vw_core::{RawFrame, Venue};

use super::auth::KalshiCreds;
use super::normalize;
use crate::conflate::ChannelClosed;
use crate::{Backoff, ConflatingSender, ConnectorEvent, ConnectorStatus, Watchdog};

/// Markets per `subscribe` command. Kalshi documents no hard number (error 26
/// exists for "per-subscription market limit"); batches keep commands small
/// and let one bad batch fail without killing the rest.
const SUBSCRIBE_BATCH: usize = 100;

#[derive(Debug)]
pub(super) enum SessionError {
    /// Downstream receiver dropped; the pipeline is shutting down.
    Closed,
    /// Handshake rejected with an auth-shaped HTTP status (401/403).
    AuthRejected(u16),
    /// Any other connect failure (DNS, TLS, non-auth HTTP status, ...).
    Connect(anyhow::Error),
}

impl From<ChannelClosed> for SessionError {
    fn from(_: ChannelClosed) -> Self {
        SessionError::Closed
    }
}

/// Run one authenticated WS session subscribed to the ticker channel for
/// `tickers`. Emits `Status(Connected)` after the handshake and resets
/// `backoff`; returns the disconnect reason once the session ends (staleness,
/// server close, stream error). Connect-phase failures return `Err` instead.
#[allow(clippy::too_many_arguments)] // internal single-caller fn; a params struct would only add ceremony
pub(super) async fn run_session(
    ws_url: &str,
    creds: &KalshiCreds,
    tickers: &[String],
    staleness: Duration,
    seq: &mut u64,
    sender: &mut ConflatingSender,
    tap: &Option<mpsc::Sender<RawFrame>>,
    backoff: &mut Backoff,
) -> Result<String, SessionError> {
    let path = url::Url::parse(ws_url)
        .map(|u| u.path().to_string())
        .map_err(|e| SessionError::Connect(anyhow::anyhow!("invalid ws_url: {e}")))?;

    let mut request = ws_url
        .into_client_request()
        .map_err(|e| SessionError::Connect(anyhow::anyhow!("building WS request: {e}")))?;
    for (name, value) in creds.auth_headers("GET", &path) {
        let value = HeaderValue::from_str(&value)
            .map_err(|e| SessionError::Connect(anyhow::anyhow!("invalid header value: {e}")))?;
        request.headers_mut().insert(name, value);
    }

    let (mut ws, _resp) = match connect_async(request).await {
        Ok(ok) => ok,
        Err(WsError::Http(resp))
            if resp.status().as_u16() == 401 || resp.status().as_u16() == 403 =>
        {
            return Err(SessionError::AuthRejected(resp.status().as_u16()));
        }
        Err(e) => return Err(SessionError::Connect(anyhow::anyhow!("WS connect: {e}"))),
    };

    tracing::info!(url = ws_url, markets = tickers.len(), "kalshi WS connected");
    backoff.reset();
    sender.try_send(ConnectorEvent::Status(ConnectorStatus::Connected))?;

    // Subscribe to the ticker channel in batches (research §2: one subscribe
    // with a `market_tickers` array, not one subscription per market).
    for (i, batch) in tickers.chunks(SUBSCRIBE_BATCH).enumerate() {
        let cmd = json!({
            "id": i + 1,
            "cmd": "subscribe",
            "params": { "channels": ["ticker"], "market_tickers": batch },
        });
        if let Err(e) = ws.send(Message::Text(cmd.to_string())).await {
            return Ok(format!("subscribe send failed: {e}"));
        }
    }

    let watchdog = Watchdog::new();
    loop {
        tokio::select! {
            frame = ws.next() => {
                // recv_ts before any parsing (spec §4).
                let recv_ts = Utc::now();
                watchdog.touch();
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        let raw: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!(error = %e, "kalshi WS sent non-JSON text frame");
                                continue;
                            }
                        };
                        if let Some(status) = raw.get("type").and_then(Value::as_str) {
                            if status == "error" {
                                tracing::warn!(msg = %raw, "kalshi WS error frame");
                            }
                        }
                        let frame = RawFrame { recv_ts, venue: Venue::Kalshi, raw_frame: raw };
                        if let Some(tap) = tap {
                            // Recording must never backpressure ingest: drop on full.
                            if tap.try_send(frame.clone()).is_err() {
                                tracing::debug!("raw-frame tap full or closed; frame not recorded");
                            }
                        }
                        for event in normalize::normalize_frame(&frame, seq) {
                            sender.try_send(event)?;
                        }
                    }
                    Some(Ok(Message::Close(close))) => {
                        return Ok(format!("server closed connection: {close:?}"));
                    }
                    // Pings are answered automatically by tungstenite; both
                    // ping and pong still count as liveness (touched above).
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Ok(format!("WS stream error: {e}")),
                    None => return Ok("WS stream ended".to_string()),
                }
            }
            _ = watchdog.wait_stale(staleness) => {
                return Ok(format!("stale: no frame for {}s", staleness.as_secs()));
            }
        }
    }
}
