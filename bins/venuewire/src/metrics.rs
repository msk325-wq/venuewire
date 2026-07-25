//! Daemon-owned Prometheus metrics (spec §9), registered on the server's
//! registry so they appear on `GET /metrics` alongside the server's WS metrics
//! and the ClickHouse sink's flush metrics.
//!
//! Two sources feed these:
//! - **inline**, updated on the hot path by the ingest consumer:
//!   `vw_ingest_to_publish_seconds`, `vw_ticks_published_total{venue}`,
//!   `vw_divergences_total`;
//! - **bridged**, polled every second by [`spawn_bridge`] from the connectors'
//!   `ConnectorMetrics` atomics and the Redis mirror's error counter:
//!   `vw_frames_received_total{venue}`, `vw_reconnects_total{venue}`,
//!   `vw_gaps_detected_total{venue}`, `vw_conflation_events_total{venue}`,
//!   `vw_redis_write_errors_total`.
//!
//! Bridged counters are surfaced as monotonic `IntCounter`s updated by delta,
//! so the `_total` series read as true counters despite living as plain atomics
//! inside the connector/mirror crates (which stay Prometheus-free).

use std::sync::Arc;
use std::time::Duration;

use prometheus::{Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry};
use vw_connectors::ConnectorMetrics;
use vw_state::MirrorHandle;

/// Handles the ingest consumer updates inline.
#[derive(Clone)]
pub struct DaemonMetrics {
    ingest_to_publish: Histogram,
    ticks_published: IntCounterVec,
    divergences: IntCounter,
}

impl DaemonMetrics {
    /// Create and register the daemon metrics on `registry`.
    pub fn register(registry: &Registry) -> anyhow::Result<Self> {
        // recv_ts → broadcast publish. Buckets ~100µs … ~0.8s (spec §9).
        let ingest_to_publish = Histogram::with_opts(
            HistogramOpts::new(
                "vw_ingest_to_publish_seconds",
                "Latency from connector frame receipt to WS broadcast publish",
            )
            .buckets(prometheus::exponential_buckets(0.0001, 2.0, 14)?),
        )?;
        let ticks_published = IntCounterVec::new(
            Opts::new(
                "vw_ticks_published_total",
                "Normalized ticks published to the WS fan-out",
            ),
            &["venue"],
        )?;
        let divergences = IntCounter::new(
            "vw_divergences_total",
            "Cross-venue divergence events emitted",
        )?;

        registry.register(Box::new(ingest_to_publish.clone()))?;
        registry.register(Box::new(ticks_published.clone()))?;
        registry.register(Box::new(divergences.clone()))?;

        Ok(Self {
            ingest_to_publish,
            ticks_published,
            divergences,
        })
    }

    /// Observe ingest→publish latency for one tick.
    pub fn observe_latency(&self, seconds: f64) {
        self.ingest_to_publish.observe(seconds.max(0.0));
    }

    /// Count one published tick for `venue`.
    pub fn tick_published(&self, venue: &str) {
        self.ticks_published.with_label_values(&[venue]).inc();
    }

    /// Count one divergence event.
    pub fn divergence(&self) {
        self.divergences.inc();
    }
}

/// One venue's bridged connector metrics.
pub struct VenueSource {
    pub venue: &'static str,
    pub metrics: Arc<ConnectorMetrics>,
}

/// Spawn a task that mirrors the connector atomics and the Redis mirror's error
/// count into registered `_total` counters, polling once a second. Bridged
/// counters need no hot-path work; a ≤1s scrape lag is irrelevant for counters.
pub fn spawn_bridge(
    registry: &Registry,
    sources: Vec<VenueSource>,
    mirror: Option<MirrorHandle>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let frames = IntCounterVec::new(
        Opts::new(
            "vw_frames_received_total",
            "Normalized events emitted by a connector (proxy for frames processed)",
        ),
        &["venue"],
    )?;
    let reconnects = IntCounterVec::new(
        Opts::new(
            "vw_reconnects_total",
            "Connector disconnect/reconnect cycles",
        ),
        &["venue"],
    )?;
    let gaps = IntCounterVec::new(
        Opts::new(
            "vw_gaps_detected_total",
            "Sequence gaps detected by a connector",
        ),
        &["venue"],
    )?;
    let conflations = IntCounterVec::new(
        Opts::new(
            "vw_conflation_events_total",
            "Ticks conflated (superseded) under channel backpressure",
        ),
        &["venue"],
    )?;
    let redis_errors = IntCounter::new(
        "vw_redis_write_errors_total",
        "Failed Redis mirror flush batches",
    )?;

    registry.register(Box::new(frames.clone()))?;
    registry.register(Box::new(reconnects.clone()))?;
    registry.register(Box::new(gaps.clone()))?;
    registry.register(Box::new(conflations.clone()))?;
    registry.register(Box::new(redis_errors.clone()))?;

    // Per-source last-seen values so we advance the counters by delta.
    let mut last: Vec<[u64; 4]> = vec![[0; 4]; sources.len()];
    let mut last_redis = 0u64;

    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            for (i, src) in sources.iter().enumerate() {
                let cur = [
                    src.metrics.frames_received(),
                    src.metrics.reconnects(),
                    src.metrics.gaps_detected(),
                    src.metrics.conflation_events(),
                ];
                advance(&frames, src.venue, cur[0], &mut last[i][0]);
                advance(&reconnects, src.venue, cur[1], &mut last[i][1]);
                advance(&gaps, src.venue, cur[2], &mut last[i][2]);
                advance(&conflations, src.venue, cur[3], &mut last[i][3]);
            }
            if let Some(m) = &mirror {
                let cur = m.write_error_count();
                if cur > last_redis {
                    redis_errors.inc_by(cur - last_redis);
                    last_redis = cur;
                }
            }
        }
    });
    Ok(handle)
}

fn advance(counter: &IntCounterVec, venue: &str, current: u64, last: &mut u64) {
    if current > *last {
        counter.with_label_values(&[venue]).inc_by(current - *last);
        *last = current;
    }
}
