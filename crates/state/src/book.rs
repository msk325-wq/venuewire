//! Hot-path book state: the latest top-of-book quote per instrument.
//!
//! `DashMap<InstrumentId, LatestQuote>` per spec §7.1. [`BookState::apply_tick`]
//! is fully synchronous — it takes one sharded-map entry lock for the duration
//! of a struct write and never holds anything across an await point.
//!
//! ## Out-of-order semantics
//!
//! `Tick::seq` is a connector-local monotonic sequence, so the ticks of any
//! single instrument carry strictly increasing `seq` values in arrival order.
//! A tick whose `seq` is **less than or equal to** the stored `seq` for that
//! instrument is therefore either a reordered stale update or a duplicate
//! delivery, and is rejected (the book keeps the newer quote; the rejection is
//! counted). This assumes connectors keep their sequence monotonic across
//! reconnects for the lifetime of the process — a daemon restart starts from an
//! empty book, so a connector seq reset cannot wedge an instrument.

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use vw_core::{InstrumentId, Tick};

/// The latest accepted top-of-book state for one instrument.
///
/// All fields are `Copy`; cloning a quote is a few machine words.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LatestQuote {
    pub yes_bid: Option<Decimal>,
    pub yes_ask: Option<Decimal>,
    pub last_price: Option<Decimal>,
    /// Venue-reported event time, if any.
    pub venue_ts: Option<DateTime<Utc>>,
    /// When the connector read the frame off the socket. Freshness checks use this.
    pub recv_ts: DateTime<Utc>,
    /// Connector-local monotonic sequence of the accepted tick.
    pub seq: u64,
    /// When the book accepted the tick, per the injected [`Clock`](crate::Clock).
    pub updated_at: DateTime<Utc>,
}

impl LatestQuote {
    /// Mid of the YES bid/ask, falling back to whichever side exists
    /// (same semantics as [`Tick::yes_mid`]).
    pub fn yes_mid(&self) -> Option<Decimal> {
        match (self.yes_bid, self.yes_ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::TWO),
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        }
    }
}

/// Concurrent latest-quote store. Shared by reference across the hot path,
/// match views, and snapshot readers; all methods take `&self`.
#[derive(Debug, Default)]
pub struct BookState {
    quotes: DashMap<InstrumentId, LatestQuote>,
    accepted: AtomicU64,
    rejected: AtomicU64,
}

impl BookState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a tick. Returns the stored quote if accepted, or `None` if the
    /// tick was rejected as out-of-order (see module docs for semantics).
    ///
    /// Synchronous; holds only a single DashMap shard lock for the duration of
    /// a struct write.
    pub fn apply_tick(&self, tick: &Tick, now: DateTime<Utc>) -> Option<LatestQuote> {
        let quote = LatestQuote {
            yes_bid: tick.yes_bid,
            yes_ask: tick.yes_ask,
            last_price: tick.last_price,
            venue_ts: tick.venue_ts,
            recv_ts: tick.recv_ts,
            seq: tick.seq,
            updated_at: now,
        };
        match self.quotes.entry(tick.instrument.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                if tick.seq <= e.get().seq {
                    self.rejected.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                e.insert(quote);
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(quote);
            }
        }
        self.accepted.fetch_add(1, Ordering::Relaxed);
        Some(quote)
    }

    /// Snapshot of one instrument's latest quote.
    pub fn quote(&self, instrument: &InstrumentId) -> Option<LatestQuote> {
        self.quotes.get(instrument).map(|q| *q)
    }

    /// Snapshot of every instrument's latest quote.
    pub fn all_quotes(&self) -> Vec<(InstrumentId, LatestQuote)> {
        self.quotes
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }

    /// Number of instruments with at least one accepted tick.
    pub fn instrument_count(&self) -> usize {
        self.quotes.len()
    }

    /// Total ticks accepted since startup.
    pub fn accepted_count(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Total ticks rejected as out-of-order since startup.
    pub fn rejected_count(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use vw_core::Venue;

    fn tick(instrument: &str, seq: u64, bid: Decimal, ask: Decimal) -> Tick {
        Tick {
            instrument: InstrumentId(instrument.to_string()),
            venue: Venue::Kalshi,
            yes_bid: Some(bid),
            yes_ask: Some(ask),
            last_price: None,
            venue_ts: None,
            recv_ts: Utc::now(),
            seq,
        }
    }

    #[test]
    fn apply_then_read_back() {
        let book = BookState::new();
        let now = Utc::now();
        let t = tick("kalshi:X", 7, dec!(0.40), dec!(0.44));
        let stored = book.apply_tick(&t, now).expect("accepted");
        assert_eq!(stored.yes_bid, Some(dec!(0.40)));
        assert_eq!(stored.updated_at, now);

        let q = book.quote(&t.instrument).expect("present");
        assert_eq!(q, stored);
        assert_eq!(q.yes_mid(), Some(dec!(0.42)));
        assert_eq!(q.seq, 7);
        assert_eq!(book.instrument_count(), 1);
        assert_eq!(book.accepted_count(), 1);
        assert_eq!(book.rejected_count(), 0);
    }

    #[test]
    fn rejects_older_and_duplicate_seq() {
        let book = BookState::new();
        let now = Utc::now();
        assert!(book
            .apply_tick(&tick("kalshi:X", 10, dec!(0.40), dec!(0.44)), now)
            .is_some());
        // strictly older
        assert!(book
            .apply_tick(&tick("kalshi:X", 9, dec!(0.10), dec!(0.20)), now)
            .is_none());
        // duplicate
        assert!(book
            .apply_tick(&tick("kalshi:X", 10, dec!(0.10), dec!(0.20)), now)
            .is_none());
        // book keeps the newest quote
        let q = book.quote(&InstrumentId("kalshi:X".into())).unwrap();
        assert_eq!(q.yes_bid, Some(dec!(0.40)));
        assert_eq!(q.seq, 10);
        assert_eq!(book.accepted_count(), 1);
        assert_eq!(book.rejected_count(), 2);
        // newer seq accepted again
        assert!(book
            .apply_tick(&tick("kalshi:X", 11, dec!(0.41), dec!(0.45)), now)
            .is_some());
        assert_eq!(book.accepted_count(), 2);
    }

    #[test]
    fn seq_tracking_is_per_instrument() {
        let book = BookState::new();
        let now = Utc::now();
        assert!(book
            .apply_tick(&tick("kalshi:A", 100, dec!(0.50), dec!(0.52)), now)
            .is_some());
        // a lower seq on a *different* instrument is fine
        assert!(book
            .apply_tick(&tick("polymarket:B", 3, dec!(0.30), dec!(0.32)), now)
            .is_some());
        assert_eq!(book.instrument_count(), 2);
        assert_eq!(book.all_quotes().len(), 2);
        assert_eq!(book.rejected_count(), 0);
    }
}
