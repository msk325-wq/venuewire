//! Venue connectors: live WebSocket ingestion normalized into [`ConnectorEvent`]s.
//!
//! v1 ships Kalshi (M1), Polymarket (M2), and a deterministic replay connector
//! reading recorded NDJSON fixtures. All connectors share the same contract:
//! push normalized events into a bounded channel and never return except on
//! fatal configuration errors.
//!
//! Venue-agnostic reliability plumbing shared by every connector:
//! - [`backoff`]: jittered exponential reconnect backoff (base 500ms, cap 30s).
//! - [`watchdog`]: staleness watchdog forcing a reconnect after a quiet period.
//! - [`conflate`]: per-instrument tick conflation when the bounded channel is
//!   full, instead of dropping events.

pub mod backoff;
pub mod conflate;
pub mod kalshi;
pub mod metrics;
pub mod polymarket;
pub mod replay;
pub mod watchdog;

pub use backoff::Backoff;
pub use conflate::{ChannelClosed, ConflatingSender};
pub use kalshi::KalshiConnector;
pub use metrics::ConnectorMetrics;
pub use polymarket::PolymarketConnector;
pub use replay::{ReplayConnector, ReplaySpeed};
pub use watchdog::Watchdog;

use async_trait::async_trait;
use tokio::sync::mpsc;
use vw_core::{Instrument, Tick, Venue};

/// Connection lifecycle signals, surfaced downstream alongside data.
#[derive(Debug, Clone)]
pub enum ConnectorStatus {
    Connected,
    Disconnected {
        reason: String,
    },
    /// A venue-provided sequence number skipped; a REST snapshot refresh is needed.
    GapDetected {
        expected: u64,
        got: u64,
    },
}

#[derive(Debug, Clone)]
pub enum ConnectorEvent {
    Tick(Tick),
    Instrument(Instrument),
    Status(ConnectorStatus),
}

#[async_trait]
pub trait VenueConnector: Send {
    fn venue(&self) -> Venue;

    /// Runs forever, sending normalized events into `tx`.
    /// Returns only on fatal configuration error; transient network failures
    /// are handled internally with jittered exponential backoff.
    async fn run(self, tx: mpsc::Sender<ConnectorEvent>) -> anyhow::Result<()>;
}
