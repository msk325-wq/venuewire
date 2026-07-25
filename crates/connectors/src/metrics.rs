//! Per-connector counters, surfaced by the daemon as Prometheus metrics
//! (spec §9). Plain atomics so connectors stay Prometheus-free; the daemon
//! owns the registry and bridges these into `vw_*{venue}` series.
//!
//! Every counter is incremented inside [`ConflatingSender`](crate::ConflatingSender),
//! the single seam every connector already routes events through, so no
//! venue-specific session code needs to know about metrics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters for one connector (one venue). Share via `Arc`: the connector
/// holds one clone (handed to its `ConflatingSender`), the daemon another.
#[derive(Debug, Default)]
pub struct ConnectorMetrics {
    frames_received: AtomicU64,
    reconnects: AtomicU64,
    gaps_detected: AtomicU64,
    conflation_events: AtomicU64,
}

impl ConnectorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Normalized events emitted by the connector (a proxy for frames
    /// processed): `vw_frames_received_total{venue}`.
    pub fn frames_received(&self) -> u64 {
        self.frames_received.load(Ordering::Relaxed)
    }

    /// Disconnect transitions (each precedes a reconnect):
    /// `vw_reconnects_total{venue}`.
    pub fn reconnects(&self) -> u64 {
        self.reconnects.load(Ordering::Relaxed)
    }

    /// Sequence-gap detections: `vw_gaps_detected_total{venue}`.
    pub fn gaps_detected(&self) -> u64 {
        self.gaps_detected.load(Ordering::Relaxed)
    }

    /// Ticks superseded by a newer tick under channel backpressure:
    /// `vw_conflation_events_total{venue}`.
    pub fn conflation_events(&self) -> u64 {
        self.conflation_events.load(Ordering::Relaxed)
    }

    pub(crate) fn record_frame(&self) {
        self.frames_received.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_gap(&self) {
        self.gaps_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_conflation(&self) {
        self.conflation_events.fetch_add(1, Ordering::Relaxed);
    }
}
