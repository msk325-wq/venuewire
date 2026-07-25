//! Per-instrument conflation on channel overflow (spec §5.1).
//!
//! When the bounded event channel fills, ticks must not be dropped-oldest.
//! Market data has last-value semantics, so [`ConflatingSender`] instead keeps
//! only the *latest* tick per [`InstrumentId`] in an overflow buffer and
//! drains it back into the channel as capacity frees. `Instrument` and
//! `Status` events are rare and order-sensitive, so they are buffered FIFO
//! and never dropped or conflated.
//!
//! Each time a buffered tick is superseded by a newer one (i.e. real data was
//! merged away) a counter increments; M6 wires it to the
//! `vw_conflation_events_total` metric.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc::{self, error::TrySendError};
use vw_core::{InstrumentId, Tick};

use crate::metrics::ConnectorMetrics;
use crate::{ConnectorEvent, ConnectorStatus};

/// The receiving side of the event channel has been dropped; the pipeline is
/// shutting down and the connector should exit.
#[derive(Debug, thiserror::Error)]
#[error("connector event channel closed")]
pub struct ChannelClosed;

/// An [`mpsc::Sender`] wrapper that conflates ticks instead of dropping them
/// when the channel is full.
///
/// Send everything through [`try_send`](ConflatingSender::try_send); backlog
/// is flushed opportunistically on every call, or explicitly via
/// [`drain`](ConflatingSender::drain) / [`flush`](ConflatingSender::flush).
/// Relative ordering of buffered events is preserved: backlog always leaves
/// before newer events, control events before conflated ticks.
#[derive(Debug)]
pub struct ConflatingSender {
    tx: mpsc::Sender<ConnectorEvent>,
    /// Instrument/Status events awaiting capacity, in arrival order.
    control_backlog: VecDeque<ConnectorEvent>,
    /// Latest tick per instrument awaiting capacity.
    tick_backlog: HashMap<InstrumentId, Tick>,
    /// Instruments in `tick_backlog`, in first-overflow order (drain order).
    tick_order: VecDeque<InstrumentId>,
    conflation_events: Arc<AtomicU64>,
    /// Optional per-venue metrics sink (spec §9); `None` in tests and any
    /// connector that opts out.
    metrics: Option<Arc<ConnectorMetrics>>,
}

impl ConflatingSender {
    /// Wrap the sending half of the bounded connector event channel.
    pub fn new(tx: mpsc::Sender<ConnectorEvent>) -> Self {
        ConflatingSender {
            tx,
            control_backlog: VecDeque::new(),
            tick_backlog: HashMap::new(),
            tick_order: VecDeque::new(),
            conflation_events: Arc::new(AtomicU64::new(0)),
            metrics: None,
        }
    }

    /// Wrap the channel and report per-venue counters into `metrics`.
    pub fn with_metrics(tx: mpsc::Sender<ConnectorEvent>, metrics: Arc<ConnectorMetrics>) -> Self {
        ConflatingSender {
            metrics: Some(metrics),
            ..ConflatingSender::new(tx)
        }
    }

    /// Send an event without blocking, conflating ticks if the channel is full.
    ///
    /// Any existing backlog is drained first so ordering is preserved. Errors
    /// only when the receiver has been dropped.
    pub fn try_send(&mut self, event: ConnectorEvent) -> Result<(), ChannelClosed> {
        if let Some(m) = &self.metrics {
            // Count each new event once (drained backlog re-sends go through
            // the private path and are not recounted).
            m.record_frame();
            match &event {
                ConnectorEvent::Status(ConnectorStatus::Disconnected { .. }) => {
                    m.record_reconnect()
                }
                ConnectorEvent::Status(ConnectorStatus::GapDetected { .. }) => m.record_gap(),
                _ => {}
            }
        }
        self.drain()?;
        if self.has_backlog() {
            // Capacity is still exhausted; queue behind the backlog.
            self.buffer(event);
            return Ok(());
        }
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(event)) => {
                self.buffer(event);
                Ok(())
            }
            Err(TrySendError::Closed(_)) => Err(ChannelClosed),
        }
    }

    /// Push as much backlog as fits into the channel right now.
    ///
    /// Returns the number of events flushed. Cheap when the backlog is empty.
    pub fn drain(&mut self) -> Result<usize, ChannelClosed> {
        let mut flushed = 0;
        while let Some(event) = self.pop_backlog() {
            match self.tx.try_send(event) {
                Ok(()) => flushed += 1,
                Err(TrySendError::Full(event)) => {
                    self.unpop_backlog(event);
                    break;
                }
                Err(TrySendError::Closed(_)) => return Err(ChannelClosed),
            }
        }
        Ok(flushed)
    }

    /// Drain the entire backlog, waiting for channel capacity as needed.
    ///
    /// Returns the number of events flushed.
    pub async fn flush(&mut self) -> Result<usize, ChannelClosed> {
        let mut flushed = self.drain()?;
        while let Some(event) = self.pop_backlog() {
            match self.tx.reserve().await {
                Ok(permit) => {
                    permit.send(event);
                    flushed += 1;
                }
                Err(_) => return Err(ChannelClosed),
            }
        }
        Ok(flushed)
    }

    /// Number of events currently held back (control + conflated ticks).
    pub fn pending(&self) -> usize {
        self.control_backlog.len() + self.tick_backlog.len()
    }

    /// Whether any events are currently held back.
    pub fn has_backlog(&self) -> bool {
        !self.control_backlog.is_empty() || !self.tick_backlog.is_empty()
    }

    /// Total ticks superseded by a newer tick for the same instrument.
    pub fn conflation_events(&self) -> u64 {
        self.conflation_events.load(Ordering::Relaxed)
    }

    /// Shared handle to the conflation counter, for wiring into the
    /// `vw_conflation_events_total` metric (M6).
    pub fn conflation_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.conflation_events)
    }

    /// Stash an event that could not be sent. Ticks conflate per instrument;
    /// everything else queues FIFO.
    fn buffer(&mut self, event: ConnectorEvent) {
        match event {
            ConnectorEvent::Tick(tick) => {
                let id = tick.instrument.clone();
                if self.tick_backlog.insert(id.clone(), tick).is_some() {
                    self.conflation_events.fetch_add(1, Ordering::Relaxed);
                    if let Some(m) = &self.metrics {
                        m.record_conflation();
                    }
                    tracing::trace!(instrument = %id, "conflated tick on full channel");
                } else {
                    self.tick_order.push_back(id);
                }
            }
            other => self.control_backlog.push_back(other),
        }
    }

    /// Next backlog event in drain order: control events first (rare,
    /// order-sensitive), then conflated ticks in first-overflow order.
    fn pop_backlog(&mut self) -> Option<ConnectorEvent> {
        if let Some(event) = self.control_backlog.pop_front() {
            return Some(event);
        }
        let id = self.tick_order.pop_front()?;
        let tick = self
            .tick_backlog
            .remove(&id)
            .expect("tick_order entry without tick_backlog entry");
        Some(ConnectorEvent::Tick(tick))
    }

    /// Return an event just taken by [`pop_backlog`] to the front of its queue.
    fn unpop_backlog(&mut self, event: ConnectorEvent) {
        match event {
            ConnectorEvent::Tick(tick) => {
                let id = tick.instrument.clone();
                self.tick_order.push_front(id.clone());
                self.tick_backlog.insert(id, tick);
            }
            other => self.control_backlog.push_front(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnectorStatus;
    use chrono::Utc;
    use vw_core::{Instrument, Venue};

    fn tick(native_id: &str, seq: u64) -> ConnectorEvent {
        ConnectorEvent::Tick(Tick {
            instrument: InstrumentId::new(Venue::Kalshi, native_id),
            venue: Venue::Kalshi,
            yes_bid: None,
            yes_ask: None,
            last_price: None,
            venue_ts: None,
            recv_ts: Utc::now(),
            seq,
        })
    }

    fn instrument(native_id: &str) -> ConnectorEvent {
        ConnectorEvent::Instrument(Instrument {
            id: InstrumentId::new(Venue::Kalshi, native_id),
            venue: Venue::Kalshi,
            title: native_id.to_string(),
            description: None,
            close_time: None,
            category: None,
            raw: serde_json::Value::Null,
        })
    }

    /// (instrument suffix, seq) of a tick event, panicking on other variants.
    fn as_tick(event: &ConnectorEvent) -> (String, u64) {
        match event {
            ConnectorEvent::Tick(t) => (t.instrument.0.clone(), t.seq),
            other => panic!("expected tick, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn passes_through_when_capacity_available() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut cs = ConflatingSender::new(tx);
        cs.try_send(tick("A", 1)).unwrap();
        cs.try_send(instrument("A")).unwrap();
        assert!(!cs.has_backlog());
        assert_eq!(cs.conflation_events(), 0);
        assert_eq!(as_tick(&rx.try_recv().unwrap()), ("kalshi:A".into(), 1));
        assert!(matches!(
            rx.try_recv().unwrap(),
            ConnectorEvent::Instrument(_)
        ));
    }

    #[tokio::test]
    async fn latest_tick_wins_per_instrument() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut cs = ConflatingSender::new(tx);
        cs.try_send(tick("A", 1)).unwrap(); // fills the channel
        for seq in 2..=5 {
            cs.try_send(tick("A", seq)).unwrap(); // conflates in the buffer
        }
        cs.try_send(tick("B", 10)).unwrap();
        cs.try_send(tick("B", 11)).unwrap();
        // Buffered ticks 2..=4 were superseded by 5; 10 was superseded by 11.
        assert_eq!(cs.conflation_events(), 4);
        assert_eq!(cs.pending(), 2);

        assert_eq!(as_tick(&rx.try_recv().unwrap()), ("kalshi:A".into(), 1));
        assert_eq!(cs.drain().unwrap(), 1);
        assert_eq!(as_tick(&rx.try_recv().unwrap()), ("kalshi:A".into(), 5));
        assert_eq!(cs.drain().unwrap(), 1);
        assert_eq!(as_tick(&rx.try_recv().unwrap()), ("kalshi:B".into(), 11));
        assert!(!cs.has_backlog());
    }

    #[tokio::test]
    async fn control_events_survive_and_drain_in_order() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut cs = ConflatingSender::new(tx);
        cs.try_send(tick("A", 1)).unwrap(); // fills the channel
        cs.try_send(tick("B", 2)).unwrap(); // buffered tick (first overflow)
        cs.try_send(ConnectorEvent::Status(ConnectorStatus::Connected))
            .unwrap();
        cs.try_send(tick("B", 3)).unwrap(); // conflates B
        cs.try_send(instrument("META")).unwrap();
        cs.try_send(tick("C", 4)).unwrap(); // buffered tick (second overflow)
        assert_eq!(cs.pending(), 4);
        assert_eq!(cs.conflation_events(), 1);

        // Drain one slot at a time (capacity 1): control FIFO first, then
        // conflated ticks in first-overflow order.
        let mut received = vec![rx.try_recv().unwrap()];
        loop {
            cs.drain().unwrap();
            match rx.try_recv() {
                Ok(event) => received.push(event),
                Err(_) => break,
            }
        }
        assert!(!cs.has_backlog());
        assert_eq!(received.len(), 5);
        assert_eq!(as_tick(&received[0]), ("kalshi:A".into(), 1));
        assert!(matches!(
            received[1],
            ConnectorEvent::Status(ConnectorStatus::Connected)
        ));
        assert!(matches!(received[2], ConnectorEvent::Instrument(_)));
        assert_eq!(as_tick(&received[3]), ("kalshi:B".into(), 3));
        assert_eq!(as_tick(&received[4]), ("kalshi:C".into(), 4));
    }

    #[tokio::test]
    async fn new_events_queue_behind_backlog() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut cs = ConflatingSender::new(tx);
        cs.try_send(tick("A", 1)).unwrap();
        cs.try_send(tick("B", 2)).unwrap(); // channel now full
        cs.try_send(tick("C", 3)).unwrap(); // buffered

        // Free both slots; the next send must drain C before D.
        rx.try_recv().unwrap();
        rx.try_recv().unwrap();
        cs.try_send(tick("D", 4)).unwrap();
        assert!(!cs.has_backlog());
        assert_eq!(as_tick(&rx.try_recv().unwrap()), ("kalshi:C".into(), 3));
        assert_eq!(as_tick(&rx.try_recv().unwrap()), ("kalshi:D".into(), 4));
    }

    #[tokio::test]
    async fn flush_awaits_capacity() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut cs = ConflatingSender::new(tx);
        cs.try_send(tick("A", 1)).unwrap();
        cs.try_send(tick("B", 2)).unwrap(); // buffered
        cs.try_send(tick("C", 3)).unwrap(); // buffered

        let (flushed, received) = tokio::join!(cs.flush(), async move {
            let mut out = Vec::new();
            for _ in 0..3 {
                out.push(rx.recv().await.unwrap());
            }
            out
        });
        assert_eq!(flushed.unwrap(), 2);
        assert!(!cs.has_backlog());
        assert_eq!(as_tick(&received[0]), ("kalshi:A".into(), 1));
        assert_eq!(as_tick(&received[1]), ("kalshi:B".into(), 2));
        assert_eq!(as_tick(&received[2]), ("kalshi:C".into(), 3));
    }

    #[tokio::test]
    async fn closed_channel_errors() {
        let (tx, rx) = mpsc::channel(1);
        let mut cs = ConflatingSender::new(tx);
        drop(rx);
        assert!(cs.try_send(tick("A", 1)).is_err());
    }
}
