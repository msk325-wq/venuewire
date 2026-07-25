//! The fan-out bus between the daemon and WS sessions.
//!
//! The daemon owns a [`PublishHandle`] and pushes every normalized tick and
//! divergence event through it; each WS session holds its own
//! `broadcast::Receiver` and filters by its subscribed topics before sending.
//! A slow session lags its receiver and loses messages — it never
//! backpressures the publisher (see decisions draft D-M5-1).

use tokio::sync::broadcast;
use vw_core::{DivergenceEvent, Tick};

/// One event on the fan-out bus. Deliberately defined here (not in
/// `vw-connectors`): the server only ever streams ticks and divergences.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Tick(Tick),
    Divergence(DivergenceEvent),
}

/// Daemon-side publisher. Cheap to clone.
#[derive(Debug, Clone)]
pub struct PublishHandle {
    tx: broadcast::Sender<StreamEvent>,
}

impl PublishHandle {
    pub(crate) fn new(tx: broadcast::Sender<StreamEvent>) -> Self {
        Self { tx }
    }

    /// Publish an event. Never blocks and never fails: with zero subscribers
    /// the event is simply not delivered anywhere.
    pub fn publish(&self, event: StreamEvent) {
        let _ = self.tx.send(event);
    }

    pub fn publish_tick(&self, tick: Tick) {
        self.publish(StreamEvent::Tick(tick));
    }

    pub fn publish_divergence(&self, event: DivergenceEvent) {
        self.publish(StreamEvent::Divergence(event));
    }

    /// Currently connected broadcast subscribers (≈ WS sessions).
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}
