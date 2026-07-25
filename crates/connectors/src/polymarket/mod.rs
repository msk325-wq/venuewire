//! Polymarket connector (M2): Gamma REST discovery + unauthenticated CLOB WSS
//! market channel (no credentials needed for any market data — see
//! docs/research/polymarket-api.md).

pub mod normalize;
pub mod rest;
mod ws;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc;
use vw_core::config::{IngestConfig, PolymarketConfig};
use vw_core::{RawFrame, Venue};

use crate::conflate::ChannelClosed;
use crate::metrics::ConnectorMetrics;
use crate::{Backoff, ConflatingSender, ConnectorEvent, ConnectorStatus, VenueConnector};
use normalize::Normalizer;
use rest::PolymarketGamma;
use ws::SessionError;

#[derive(Debug)]
pub struct PolymarketConnector {
    cfg: PolymarketConfig,
    staleness: Duration,
    raw_tap: Option<mpsc::Sender<RawFrame>>,
    metrics: Option<Arc<ConnectorMetrics>>,
}

impl PolymarketConnector {
    pub fn new(cfg: PolymarketConfig, ingest: &IngestConfig) -> Self {
        PolymarketConnector {
            cfg,
            staleness: Duration::from_secs(ingest.staleness_reconnect_secs),
            raw_tap: None,
            metrics: None,
        }
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

    /// One Gamma discovery pass over all configured tags: record raw frames,
    /// emit `Instrument` for first-seen markets and a snapshot `Tick` for
    /// every market (from `outcomePrices`/`bestBid`/`bestAsk`), and register
    /// token ids with the normalizer. Returns the yes-leg token ids known so
    /// far (the WSS subscribe list).
    async fn discover(
        &self,
        gamma: &PolymarketGamma,
        tag_ids: &mut HashMap<String, String>,
        normalizer: &mut Normalizer,
        seq: &mut u64,
        sender: &mut ConflatingSender,
        known: &mut HashSet<String>,
    ) -> Result<Vec<String>, StepError> {
        let mut seen_conditions = HashSet::new();
        let mut budget = self.cfg.max_markets;
        for slug in &self.cfg.tags {
            if budget == 0 {
                break;
            }
            // Slug → id resolutions are cached across reconnects.
            let tag_id = match tag_ids.get(slug) {
                Some(id) => id.clone(),
                None => match gamma
                    .resolve_tag(slug)
                    .await
                    .map_err(StepError::Transient)?
                {
                    Some(id) => {
                        tag_ids.insert(slug.clone(), id.clone());
                        id
                    }
                    None => continue, // unknown slug, already warned
                },
            };
            let markets = gamma
                .active_markets(&tag_id, budget)
                .await
                .map_err(StepError::Transient)?;
            tracing::debug!(
                tag = slug,
                markets = markets.len(),
                "polymarket Gamma snapshot"
            );
            for market in markets {
                // Tags can overlap; process each market once per pass.
                if let Some(condition_id) = market.get("conditionId").and_then(|v| v.as_str()) {
                    if !seen_conditions.insert(condition_id.to_string()) {
                        continue;
                    }
                }
                budget = budget.saturating_sub(1);
                // recv_ts stamped when the payload is in hand, before parsing.
                let frame = RawFrame {
                    recv_ts: Utc::now(),
                    venue: Venue::Polymarket,
                    raw_frame: market,
                };
                if let Some(tap) = &self.raw_tap {
                    if tap.try_send(frame.clone()).is_err() {
                        tracing::debug!("raw-frame tap full or closed; frame not recorded");
                    }
                }
                for event in normalizer.normalize_frame(&frame, seq) {
                    match &event {
                        ConnectorEvent::Instrument(inst) => {
                            let native = inst.id.0.clone();
                            if known.insert(native) {
                                sender.try_send(event)?;
                            }
                            // else: metadata already emitted; skip the re-discovery duplicate.
                        }
                        _ => sender.try_send(event)?,
                    }
                }
            }
        }
        Ok(normalizer.yes_tokens().to_vec())
    }
}

#[async_trait]
impl VenueConnector for PolymarketConnector {
    fn venue(&self) -> Venue {
        Venue::Polymarket
    }

    /// Supervised loop: rediscover via Gamma, connect WSS, stream until the
    /// session ends, back off, repeat. Only returns once the downstream
    /// receiver is gone (clean pipeline shutdown).
    async fn run(self, tx: mpsc::Sender<ConnectorEvent>) -> anyhow::Result<()> {
        let gamma = PolymarketGamma::new(&self.cfg.gamma_url)?;
        let mut sender = match &self.metrics {
            Some(m) => ConflatingSender::with_metrics(tx, Arc::clone(m)),
            None => ConflatingSender::new(tx),
        };
        let mut seq = 0u64;
        let mut known = HashSet::new();
        let mut tag_ids = HashMap::new();
        // The normalizer's token map persists across reconnects so mid-session
        // frames for previously discovered markets always attribute.
        let mut normalizer = Normalizer::new();
        let mut backoff = Backoff::default();

        loop {
            // Fresh discovery each (re)connect: seeds/updates the snapshot and
            // picks up newly listed markets for the subscribe list.
            let tokens = match self
                .discover(
                    &gamma,
                    &mut tag_ids,
                    &mut normalizer,
                    &mut seq,
                    &mut sender,
                    &mut known,
                )
                .await
            {
                Ok(tokens) if tokens.is_empty() => {
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        tags = ?self.cfg.tags,
                        retry_in = ?delay,
                        "polymarket discovery found no subscribable markets"
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                Ok(tokens) => tokens,
                Err(StepError::Closed) => break,
                Err(StepError::Transient(e)) => {
                    let delay = backoff.next_delay();
                    tracing::warn!(error = %format!("{e:#}"), retry_in = ?delay, "polymarket discovery failed");
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            match ws::run_session(
                &self.cfg.ws_url,
                &tokens,
                self.staleness,
                &mut seq,
                &mut sender,
                &self.raw_tap,
                &mut normalizer,
                &mut backoff,
            )
            .await
            {
                Ok(reason) => {
                    tracing::warn!(%reason, "polymarket WS session ended; reconnecting");
                    if sender
                        .try_send(ConnectorEvent::Status(ConnectorStatus::Disconnected {
                            reason,
                        }))
                        .is_err()
                    {
                        break;
                    }
                }
                Err(SessionError::Closed) => break,
                Err(SessionError::Connect(e)) => {
                    tracing::warn!(error = %format!("{e:#}"), "polymarket WS connect failed");
                }
            }
            let delay = backoff.next_delay();
            tracing::info!(retry_in = ?delay, "polymarket reconnect backoff");
            tokio::time::sleep(delay).await;
        }
        tracing::info!("polymarket connector exiting: event channel closed");
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
