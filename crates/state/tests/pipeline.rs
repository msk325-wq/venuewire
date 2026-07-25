//! End-to-end M4 pipeline test: synthetic tick sequence for a 2-leg match
//! driven through [`PipelineState`], asserting the exact `DivergenceEvent`s,
//! book/view snapshots, and mirror dirty-tracking. Deterministic time via
//! [`ManualClock`]; no wall-clock sleeps.

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use vw_core::config::DivergenceConfig;
use vw_core::{InstrumentId, MatchConfidence, MatchMethod, MatchedMarket, Tick, Venue};
use vw_state::{ManualClock, MirrorHandle, PipelineState};

const KALSHI_LEG: &str = "kalshi:KXFED-26JUL";
const POLY_LEG: &str = "polymarket:0xfed26jul";

fn iid(s: &str) -> InstrumentId {
    InstrumentId(s.to_string())
}

fn fed_match() -> MatchedMarket {
    MatchedMarket {
        match_id: "fed-cut-jul".into(),
        legs: vec![iid(KALSHI_LEG), iid(POLY_LEG)],
        confidence: MatchConfidence::Exact,
        method: MatchMethod::Rule,
    }
}

fn tick(
    instrument: &str,
    venue: Venue,
    seq: u64,
    bid: Decimal,
    ask: Decimal,
    recv_ts: DateTime<Utc>,
) -> Tick {
    Tick {
        instrument: iid(instrument),
        venue,
        yes_bid: Some(bid),
        yes_ask: Some(ask),
        last_price: None,
        venue_ts: None,
        recv_ts,
        seq,
    }
}

fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-07-18T14:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

#[test]
fn two_leg_match_end_to_end() {
    let clock = Arc::new(ManualClock::new(t0()));
    let mirror = MirrorHandle::new();
    let state = PipelineState::new(
        DivergenceConfig::default(), // threshold 0.03, freshness 60s, debounce 10s
        &[fed_match()],
        clock.clone(),
        Some(mirror.clone()),
    );
    assert_eq!(state.match_count(), 1);

    // t0: kalshi leg arrives — mid 0.42, other leg unknown → no event
    let events = state.apply(&tick(
        KALSHI_LEG,
        Venue::Kalshi,
        1,
        dec!(0.40),
        dec!(0.44),
        t0(),
    ));
    assert!(events.is_empty());
    // quote and match view were marked dirty for the mirror
    assert_eq!(mirror.pending_len(), 2);

    // t0+1s: polymarket leg — mid 0.37, spread 0.05 ≥ 0.03, both fresh → event
    clock.advance(Duration::seconds(1));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        1,
        dec!(0.36),
        dec!(0.38),
        t0() + Duration::seconds(1),
    ));
    assert_eq!(events.len(), 1);
    let ev = &events[0];
    assert_eq!(ev.match_id, "fed-cut-jul");
    assert_eq!(ev.spread, dec!(0.05));
    assert_eq!(ev.detected_at, t0() + Duration::seconds(1));
    assert_eq!(
        ev.legs,
        vec![(iid(KALSHI_LEG), dec!(0.42)), (iid(POLY_LEG), dec!(0.37))]
    );

    // t0+3s: spread creeps to 0.055 (< +0.01 since last emission) → debounced
    clock.advance(Duration::seconds(2));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        2,
        dec!(0.355),
        dec!(0.375),
        t0() + Duration::seconds(3),
    ));
    assert!(events.is_empty());

    // t0+5s: spread widens to 0.06 (= +0.01 since last emission) → emits
    clock.advance(Duration::seconds(2));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        3,
        dec!(0.35),
        dec!(0.37),
        t0() + Duration::seconds(5),
    ));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].spread, dec!(0.06));
    assert_eq!(events[0].detected_at, t0() + Duration::seconds(5));

    // out-of-order kalshi tick (stale seq): dropped — no event, book unchanged
    let events = state.apply(&tick(
        KALSHI_LEG,
        Venue::Kalshi,
        0,
        dec!(0.99),
        dec!(0.99),
        t0() + Duration::seconds(5),
    ));
    assert!(events.is_empty());
    let q = state.quote(&iid(KALSHI_LEG)).unwrap();
    assert_eq!(q.yes_bid, Some(dec!(0.40)));
    assert_eq!(q.seq, 1);
    assert_eq!(state.tick_counts(), (4, 1));

    // t0+16s: debounce window (10s) elapsed; same 0.06 spread emits again
    clock.advance(Duration::seconds(11));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        4,
        dec!(0.35),
        dec!(0.37),
        t0() + Duration::seconds(16),
    ));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].spread, dec!(0.06));
    assert_eq!(events[0].detected_at, t0() + Duration::seconds(16));

    // t0+86s: kalshi leg is now 86s old (> 60s freshness) → no event even
    // though the spread still crosses the threshold
    clock.advance(Duration::seconds(70));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        5,
        dec!(0.35),
        dec!(0.37),
        t0() + Duration::seconds(86),
    ));
    assert!(events.is_empty());

    // snapshot accessors reflect the latest state
    let view = state.match_view("fed-cut-jul").unwrap();
    assert_eq!(view.best_yes_bid, Some(dec!(0.40))); // kalshi bid > poly bid
    assert_eq!(view.best_yes_ask, Some(dec!(0.37))); // poly ask < kalshi ask
    assert_eq!(view.legs[0].mid, Some(dec!(0.42)));
    assert_eq!(view.legs[1].mid, Some(dec!(0.36)));
    assert_eq!(state.instrument_count(), 2);
    assert_eq!(state.all_quotes().len(), 2);
    assert_eq!(state.all_match_views().len(), 1);
}

#[test]
fn match_refresh_swaps_views_and_clears_debounce() {
    let clock = Arc::new(ManualClock::new(t0()));
    let state = PipelineState::new(
        DivergenceConfig::default(),
        &[fed_match()],
        clock.clone(),
        None,
    );

    state.apply(&tick(
        KALSHI_LEG,
        Venue::Kalshi,
        1,
        dec!(0.40),
        dec!(0.44),
        t0(),
    ));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        1,
        dec!(0.36),
        dec!(0.38),
        t0(),
    ));
    assert_eq!(events.len(), 1);

    // matcher refresh drops the match: no more views or events, book intact
    state.set_matches(&[]);
    assert_eq!(state.match_count(), 0);
    assert!(state.match_view("fed-cut-jul").is_none());
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        2,
        dec!(0.30),
        dec!(0.32),
        t0(),
    ));
    assert!(events.is_empty());
    assert_eq!(state.instrument_count(), 2);

    // the match comes back: views rebuild from the live book and the old
    // debounce record is gone, so the divergence emits immediately
    clock.advance(Duration::seconds(1));
    state.set_matches(&[fed_match()]);
    assert_eq!(state.match_count(), 1);
    let view = state.match_view("fed-cut-jul").unwrap();
    assert_eq!(view.legs[1].mid, Some(dec!(0.31)));
    let events = state.apply(&tick(
        POLY_LEG,
        Venue::Polymarket,
        3,
        dec!(0.30),
        dec!(0.32),
        t0() + Duration::seconds(1),
    ));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].spread, dec!(0.11));
}
