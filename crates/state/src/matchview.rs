//! Per-match best-price views over the book (spec §7.1).
//!
//! [`MatchIndex`] is an immutable snapshot: `instrument → match_ids` reverse
//! index plus `match_id → MatchedMarket`. [`MatchViews`] owns the current
//! index (swappable when the matcher refreshes) and a live [`MatchView`] per
//! match, recomputed incrementally on each tick that touches a matched
//! instrument. Rebuilding the match set never disturbs [`BookState`]; new
//! views are recomputed from the quotes already in the book.
//!
//! The caller (matcher/daemon) is responsible for filtering out `Review` and
//! rejected matches before handing the slice over; this module only drops
//! degenerate matches with fewer than two legs.

use crate::book::BookState;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use vw_core::{InstrumentId, MatchedMarket};

/// One leg of a match, as currently known to the book.
#[derive(Debug, Clone, Serialize)]
pub struct LegView {
    pub instrument: InstrumentId,
    pub yes_bid: Option<Decimal>,
    pub yes_ask: Option<Decimal>,
    /// Mid of the YES bid/ask (falls back to the one-sided price).
    pub mid: Option<Decimal>,
    /// `recv_ts` of the latest accepted tick for this leg; `None` if the book
    /// has never seen the instrument. Divergence freshness checks use this.
    pub last_update: Option<DateTime<Utc>>,
}

/// Live cross-venue view of one matched market.
#[derive(Debug, Clone, Serialize)]
pub struct MatchView {
    pub match_id: String,
    /// In the leg order of the underlying [`MatchedMarket`].
    pub legs: Vec<LegView>,
    /// Highest YES bid across legs (best price for a seller).
    pub best_yes_bid: Option<Decimal>,
    /// Lowest YES ask across legs (best price for a buyer).
    pub best_yes_ask: Option<Decimal>,
}

/// Immutable snapshot of the active match set with a reverse index.
#[derive(Debug, Default)]
pub struct MatchIndex {
    by_id: HashMap<String, Arc<MatchedMarket>>,
    by_instrument: HashMap<InstrumentId, Vec<String>>,
}

impl MatchIndex {
    /// Build from the matcher's output, dropping matches with fewer than two
    /// legs. Caller filters review/rejected matches upstream.
    pub fn build(matches: &[MatchedMarket]) -> Self {
        let mut by_id = HashMap::new();
        let mut by_instrument: HashMap<InstrumentId, Vec<String>> = HashMap::new();
        for m in matches {
            if m.legs.len() < 2 {
                tracing::warn!(match_id = %m.match_id, legs = m.legs.len(),
                    "ignoring matched market with fewer than two legs");
                continue;
            }
            for leg in &m.legs {
                let ids = by_instrument.entry(leg.clone()).or_default();
                if !ids.contains(&m.match_id) {
                    ids.push(m.match_id.clone());
                }
            }
            by_id.insert(m.match_id.clone(), Arc::new(m.clone()));
        }
        Self {
            by_id,
            by_instrument,
        }
    }

    /// Matches that include `instrument` as a leg.
    pub fn matches_for(&self, instrument: &InstrumentId) -> Vec<Arc<MatchedMarket>> {
        self.by_instrument
            .get(instrument)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.by_id.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get(&self, match_id: &str) -> Option<Arc<MatchedMarket>> {
        self.by_id.get(match_id).cloned()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

/// The live view store: current [`MatchIndex`] plus one [`MatchView`] per match.
#[derive(Debug)]
pub struct MatchViews {
    index: RwLock<Arc<MatchIndex>>,
    views: DashMap<String, MatchView>,
}

impl MatchViews {
    /// Build the index and seed a view per match from whatever the book holds.
    pub fn new(matches: &[MatchedMarket], book: &BookState) -> Self {
        let this = Self {
            index: RwLock::new(Arc::new(MatchIndex::default())),
            views: DashMap::new(),
        };
        this.rebuild(matches, book);
        this
    }

    /// Swap in a fresh match set (matcher refresh) without disturbing book
    /// state. Views for removed matches are dropped; views for new or kept
    /// matches are recomputed from the book.
    pub fn rebuild(&self, matches: &[MatchedMarket], book: &BookState) {
        let index = Arc::new(MatchIndex::build(matches));
        *self.index.write().expect("match index lock poisoned") = Arc::clone(&index);
        self.views.retain(|id, _| index.by_id.contains_key(id));
        for m in index.by_id.values() {
            self.views.insert(m.match_id.clone(), compute_view(m, book));
        }
    }

    /// Incrementally recompute the views of every match touching `instrument`.
    /// Returns the updated views (empty if the instrument is unmatched).
    ///
    /// Synchronous; takes a brief `RwLock` read plus DashMap shard locks.
    pub fn on_tick(&self, instrument: &InstrumentId, book: &BookState) -> Vec<MatchView> {
        let index = Arc::clone(&self.index.read().expect("match index lock poisoned"));
        let touched = index.matches_for(instrument);
        let mut updated = Vec::with_capacity(touched.len());
        for m in touched {
            let view = compute_view(&m, book);
            self.views.insert(m.match_id.clone(), view.clone());
            updated.push(view);
        }
        updated
    }

    /// Snapshot of one match's view.
    pub fn get(&self, match_id: &str) -> Option<MatchView> {
        self.views.get(match_id).map(|v| v.clone())
    }

    /// Snapshot of every match's view.
    pub fn all(&self) -> Vec<MatchView> {
        self.views.iter().map(|e| e.value().clone()).collect()
    }

    /// Number of active (>= 2 leg) matches.
    pub fn match_count(&self) -> usize {
        self.index.read().expect("match index lock poisoned").len()
    }
}

fn compute_view(m: &MatchedMarket, book: &BookState) -> MatchView {
    let legs: Vec<LegView> = m
        .legs
        .iter()
        .map(|instrument| match book.quote(instrument) {
            Some(q) => LegView {
                instrument: instrument.clone(),
                yes_bid: q.yes_bid,
                yes_ask: q.yes_ask,
                mid: q.yes_mid(),
                last_update: Some(q.recv_ts),
            },
            None => LegView {
                instrument: instrument.clone(),
                yes_bid: None,
                yes_ask: None,
                mid: None,
                last_update: None,
            },
        })
        .collect();
    let best_yes_bid = legs.iter().filter_map(|l| l.yes_bid).max();
    let best_yes_ask = legs.iter().filter_map(|l| l.yes_ask).min();
    MatchView {
        match_id: m.match_id.clone(),
        legs,
        best_yes_bid,
        best_yes_ask,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use vw_core::{MatchConfidence, MatchMethod, Tick, Venue};

    fn iid(s: &str) -> InstrumentId {
        InstrumentId(s.to_string())
    }

    fn matched(match_id: &str, legs: &[&str]) -> MatchedMarket {
        MatchedMarket {
            match_id: match_id.to_string(),
            legs: legs.iter().map(|l| iid(l)).collect(),
            confidence: MatchConfidence::Exact,
            method: MatchMethod::Rule,
        }
    }

    fn tick(instrument: &str, seq: u64, bid: Decimal, ask: Decimal) -> Tick {
        Tick {
            instrument: iid(instrument),
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
    fn index_drops_single_leg_matches() {
        let index = MatchIndex::build(&[
            matched("good", &["kalshi:A", "polymarket:B"]),
            matched("degenerate", &["kalshi:C"]),
        ]);
        assert_eq!(index.len(), 1);
        assert!(index.get("good").is_some());
        assert!(index.get("degenerate").is_none());
        assert!(index.matches_for(&iid("kalshi:C")).is_empty());
        assert_eq!(index.matches_for(&iid("kalshi:A")).len(), 1);
    }

    #[test]
    fn best_prices_across_legs_with_realistic_decimals() {
        let book = BookState::new();
        let now = Utc::now();
        // Kalshi leg: 0.42 / 0.44 — Polymarket leg: 0.40 / 0.435
        book.apply_tick(&tick("kalshi:FED", 1, dec!(0.42), dec!(0.44)), now);
        book.apply_tick(&tick("polymarket:0xfed", 1, dec!(0.40), dec!(0.435)), now);

        let views = MatchViews::new(
            &[matched("fed-cut", &["kalshi:FED", "polymarket:0xfed"])],
            &book,
        );
        let v = views.get("fed-cut").unwrap();
        assert_eq!(v.best_yes_bid, Some(dec!(0.42)));
        assert_eq!(v.best_yes_ask, Some(dec!(0.435)));
        assert_eq!(v.legs[0].mid, Some(dec!(0.43)));
        assert_eq!(v.legs[1].mid, Some(dec!(0.4175)));
        assert!(v.legs.iter().all(|l| l.last_update.is_some()));
    }

    #[test]
    fn on_tick_updates_only_touching_matches() {
        let book = BookState::new();
        let now = Utc::now();
        let views = MatchViews::new(
            &[
                matched("m1", &["kalshi:A", "polymarket:B"]),
                matched("m2", &["kalshi:C", "polymarket:D"]),
            ],
            &book,
        );

        // unmatched instrument: no views updated
        book.apply_tick(&tick("kalshi:UNRELATED", 1, dec!(0.5), dec!(0.52)), now);
        assert!(views.on_tick(&iid("kalshi:UNRELATED"), &book).is_empty());

        // matched instrument: exactly m1 updated
        book.apply_tick(&tick("kalshi:A", 1, dec!(0.30), dec!(0.34)), now);
        let updated = views.on_tick(&iid("kalshi:A"), &book);
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].match_id, "m1");
        assert_eq!(updated[0].legs[0].mid, Some(dec!(0.32)));
        // the other leg has no quote yet
        assert_eq!(updated[0].legs[1].mid, None);
        assert!(updated[0].legs[1].last_update.is_none());
        // and the stored snapshot matches
        assert_eq!(views.get("m1").unwrap().legs[0].mid, Some(dec!(0.32)));
        assert_eq!(views.get("m2").unwrap().legs[0].mid, None);
    }

    #[test]
    fn rebuild_swaps_match_set_without_touching_book() {
        let book = BookState::new();
        let now = Utc::now();
        book.apply_tick(&tick("kalshi:A", 1, dec!(0.60), dec!(0.62)), now);
        book.apply_tick(&tick("polymarket:B", 1, dec!(0.55), dec!(0.57)), now);

        let views = MatchViews::new(&[matched("old", &["kalshi:A", "polymarket:B"])], &book);
        assert!(views.get("old").is_some());

        views.rebuild(&[matched("new", &["kalshi:A", "polymarket:B"])], &book);
        assert!(views.get("old").is_none());
        // new match immediately sees the quotes already in the book
        let v = views.get("new").unwrap();
        assert_eq!(v.best_yes_bid, Some(dec!(0.60)));
        assert_eq!(v.legs[1].mid, Some(dec!(0.56)));
        assert_eq!(views.match_count(), 1);
        assert_eq!(book.instrument_count(), 2);

        // reverse index follows the swap
        let updated = views.on_tick(&iid("kalshi:A"), &book);
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].match_id, "new");
    }

    #[test]
    fn three_leg_match_views() {
        let book = BookState::new();
        let now = Utc::now();
        book.apply_tick(&tick("kalshi:A", 1, dec!(0.50), dec!(0.52)), now);
        book.apply_tick(&tick("polymarket:B", 1, dec!(0.44), dec!(0.46)), now);
        book.apply_tick(&tick("polymarket:C", 1, dec!(0.48), dec!(0.49)), now);

        let views = MatchViews::new(
            &[matched(
                "tri",
                &["kalshi:A", "polymarket:B", "polymarket:C"],
            )],
            &book,
        );
        let v = views.get("tri").unwrap();
        assert_eq!(v.legs.len(), 3);
        assert_eq!(v.best_yes_bid, Some(dec!(0.50)));
        assert_eq!(v.best_yes_ask, Some(dec!(0.46)));
    }
}
