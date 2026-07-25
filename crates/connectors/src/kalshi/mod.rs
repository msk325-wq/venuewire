//! Kalshi connector (M1): REST discovery + authenticated WSS ticker stream,
//! with a degraded REST-polling mode when no API credentials are configured
//! (Kalshi's WSS requires RSA-PSS auth even for public market data — see
//! docs/research/kalshi-api.md §2 and decisions.md D6).

pub mod auth;
pub mod normalize;
pub mod rest;
mod ws;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc;
use vw_core::config::{IngestConfig, KalshiConfig};
use vw_core::{RawFrame, Venue};

use crate::conflate::ChannelClosed;
use crate::metrics::ConnectorMetrics;
use crate::{Backoff, ConflatingSender, ConnectorEvent, ConnectorStatus, VenueConnector};
use auth::KalshiCreds;
use rest::KalshiRest;
use ws::SessionError;

/// Consecutive auth-rejected handshakes before giving up on WSS and switching
/// to the degraded REST-polling mode for the rest of the process lifetime.
const MAX_AUTH_FAILURES: u32 = 3;

#[derive(Debug)]
pub struct KalshiConnector {
    cfg: KalshiConfig,
    staleness: Duration,
    creds: Option<KalshiCreds>,
    raw_tap: Option<mpsc::Sender<RawFrame>>,
    metrics: Option<Arc<ConnectorMetrics>>,
}

impl KalshiConnector {
    /// Build a connector, loading credentials from the environment
    /// (`KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PATH`/`KALSHI_PRIVATE_KEY_PEM`).
    ///
    /// Absent credentials are fine (degraded REST polling); present-but-broken
    /// credentials are a configuration error.
    pub fn from_env(cfg: KalshiConfig, ingest: &IngestConfig) -> anyhow::Result<Self> {
        let creds = KalshiCreds::from_env()?;
        Ok(KalshiConnector {
            cfg,
            staleness: Duration::from_secs(ingest.staleness_reconnect_secs),
            creds,
            raw_tap: None,
            metrics: None,
        })
    }

    /// Attach a raw-frame tap (recorder). Zero-cost when not attached; frames
    /// are dropped (never blocked on) if the tap can't keep up.
    pub fn with_raw_tap(mut self, tap: mpsc::Sender<RawFrame>) -> Self {
        self.raw_tap = Some(tap);
        self
    }

    /// Report per-venue counters (spec §9) into `metrics`.
    pub fn with_metrics(mut self, metrics: Arc<ConnectorMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// One REST pass over all configured series: record raw frames, emit
    /// `Instrument` for first-seen markets and a snapshot `Tick` for every
    /// market. Returns the native tickers seen (WS subscribe list).
    async fn poll_series(
        &self,
        rest: &KalshiRest,
        seq: &mut u64,
        sender: &mut ConflatingSender,
        known: &mut HashSet<String>,
    ) -> Result<Vec<String>, StepError> {
        let mut tickers = Vec::new();
        for series in &self.cfg.series_filters {
            let markets = rest
                .open_markets(series)
                .await
                .map_err(StepError::Transient)?;
            tracing::debug!(series, markets = markets.len(), "kalshi REST snapshot");
            for market in markets {
                // recv_ts stamped when the payload is in hand, before parsing.
                let frame = RawFrame {
                    recv_ts: Utc::now(),
                    venue: Venue::Kalshi,
                    raw_frame: market,
                };
                if let Some(tap) = &self.raw_tap {
                    if tap.try_send(frame.clone()).is_err() {
                        tracing::debug!("raw-frame tap full or closed; frame not recorded");
                    }
                }
                for event in normalize::normalize_frame(&frame, seq) {
                    match &event {
                        ConnectorEvent::Instrument(inst) => {
                            let native = inst.id.0.clone();
                            if known.insert(native.clone()) {
                                sender.try_send(event)?;
                            }
                            // else: metadata already emitted; skip the re-poll duplicate.
                        }
                        _ => sender.try_send(event)?,
                    }
                }
                if let Some(t) = frame_ticker(&frame) {
                    tickers.push(t);
                }
            }
        }
        Ok(tickers)
    }

    /// Degraded no-credential mode: REST re-poll loop. Runs until the
    /// downstream channel closes.
    async fn poll_loop(
        &self,
        rest: &KalshiRest,
        seq: &mut u64,
        sender: &mut ConflatingSender,
        known: &mut HashSet<String>,
    ) -> Result<(), ChannelClosed> {
        let poll_interval = Duration::from_secs(self.cfg.poll_secs.max(1));
        let mut backoff = Backoff::default();
        let mut connected = false;
        loop {
            match self.poll_series(rest, seq, sender, known).await {
                Ok(tickers) => {
                    if !connected {
                        connected = true;
                        backoff.reset();
                        sender.try_send(ConnectorEvent::Status(ConnectorStatus::Connected))?;
                        tracing::info!(markets = tickers.len(), "kalshi REST polling active");
                    }
                    tokio::time::sleep(poll_interval).await;
                }
                Err(StepError::Closed) => return Err(ChannelClosed),
                Err(StepError::Transient(e)) => {
                    if connected {
                        connected = false;
                        sender.try_send(ConnectorEvent::Status(ConnectorStatus::Disconnected {
                            reason: format!("REST poll failed: {e:#}"),
                        }))?;
                    }
                    let delay = backoff.next_delay();
                    tracing::warn!(error = %format!("{e:#}"), retry_in = ?delay, "kalshi REST poll failed");
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Supervised WSS loop: rediscover via REST, connect, stream until the
    /// session ends, back off, repeat. Repeated auth rejections downgrade to
    /// REST polling instead of hammering the handshake forever.
    async fn ws_loop(
        &self,
        creds: &KalshiCreds,
        rest: &KalshiRest,
        seq: &mut u64,
        sender: &mut ConflatingSender,
        known: &mut HashSet<String>,
    ) -> Result<(), ChannelClosed> {
        let mut backoff = Backoff::default();
        let mut auth_failures = 0u32;
        loop {
            // Fresh discovery each (re)connect: seeds/updates the snapshot and
            // picks up newly listed markets for the subscribe list.
            let tickers = match self.poll_series(rest, seq, sender, known).await {
                Ok(t) => t,
                Err(StepError::Closed) => return Err(ChannelClosed),
                Err(StepError::Transient(e)) => {
                    let delay = backoff.next_delay();
                    tracing::warn!(error = %format!("{e:#}"), retry_in = ?delay, "kalshi discovery failed");
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            match ws::run_session(
                &self.cfg.ws_url,
                creds,
                &tickers,
                self.staleness,
                seq,
                sender,
                &self.raw_tap,
                &mut backoff,
            )
            .await
            {
                Ok(reason) => {
                    auth_failures = 0;
                    tracing::warn!(%reason, "kalshi WS session ended; reconnecting");
                    sender.try_send(ConnectorEvent::Status(ConnectorStatus::Disconnected {
                        reason,
                    }))?;
                }
                Err(SessionError::Closed) => return Err(ChannelClosed),
                Err(SessionError::AuthRejected(status)) => {
                    auth_failures += 1;
                    tracing::error!(
                        status,
                        attempt = auth_failures,
                        "kalshi WS handshake rejected — check KALSHI_API_KEY_ID / \
                         KALSHI_PRIVATE_KEY_PATH (RSA-PSS key from account settings)"
                    );
                    if auth_failures >= MAX_AUTH_FAILURES {
                        tracing::error!(
                            "kalshi WS auth rejected {MAX_AUTH_FAILURES} times; \
                             falling back to degraded REST polling for this run"
                        );
                        return self.poll_loop(rest, seq, sender, known).await;
                    }
                }
                Err(SessionError::Connect(e)) => {
                    tracing::warn!(error = %format!("{e:#}"), "kalshi WS connect failed");
                }
            }
            let delay = backoff.next_delay();
            tracing::info!(retry_in = ?delay, "kalshi reconnect backoff");
            tokio::time::sleep(delay).await;
        }
    }
}

#[async_trait]
impl VenueConnector for KalshiConnector {
    fn venue(&self) -> Venue {
        Venue::Kalshi
    }

    async fn run(self, tx: mpsc::Sender<ConnectorEvent>) -> anyhow::Result<()> {
        let rest = KalshiRest::new(&self.cfg.rest_url)?;
        let mut sender = match &self.metrics {
            Some(m) => ConflatingSender::with_metrics(tx, Arc::clone(m)),
            None => ConflatingSender::new(tx),
        };
        let mut seq = 0u64;
        let mut known = HashSet::new();

        let result = match &self.creds {
            Some(creds) => {
                self.ws_loop(creds, &rest, &mut seq, &mut sender, &mut known)
                    .await
            }
            None => {
                // The single loud warning for the degraded mode (D6).
                tracing::warn!(
                    poll_secs = self.cfg.poll_secs,
                    "no Kalshi API credentials (KALSHI_API_KEY_ID unset); Kalshi WSS \
                     requires auth even for market data — falling back to degraded \
                     REST polling"
                );
                self.poll_loop(&rest, &mut seq, &mut sender, &mut known)
                    .await
            }
        };
        // Both loops only return once the downstream receiver is gone
        // (ChannelClosed); that is a clean pipeline shutdown, not an error.
        let _ = result;
        tracing::info!("kalshi connector exiting: event channel closed");
        Ok(())
    }
}

/// Internal step outcome: distinguish "pipeline shut down" from "retry later".
#[derive(Debug)]
enum StepError {
    Closed,
    Transient(anyhow::Error),
}

impl From<ChannelClosed> for StepError {
    fn from(_: ChannelClosed) -> Self {
        StepError::Closed
    }
}

/// Native venue ticker of a REST market frame (subscribe key), if present.
fn frame_ticker(frame: &RawFrame) -> Option<String> {
    frame
        .raw_frame
        .get("ticker")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}
