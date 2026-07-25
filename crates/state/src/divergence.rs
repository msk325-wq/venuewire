//! Divergence detector (spec §7.2).
//!
//! Consumes [`MatchView`] updates. For each update it considers the legs that
//! have a mid **and** are fresh (age of the leg's `last_update`, i.e. the tick
//! `recv_ts`, strictly less than `freshness_secs`). With at least two fresh
//! legs, the spread is the **maximum pairwise** `|mid_i - mid_j|`; for the
//! canonical 2-leg case that is exactly `|mid_a - mid_b|`, and the freshness
//! rule degenerates to "both legs fresher than 60s".
//!
//! Debounce, per spec: at most one event per match per `debounce_secs`,
//! **unless** the spread has widened by at least `0.01` since the spread we
//! last emitted for that match — a widening event also restarts the debounce
//! window. Spreads back below the threshold never emit and do not clear the
//! debounce record.
//!
//! Time is injected by the caller (`now`), so tests are fully deterministic.

use crate::matchview::MatchView;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use vw_core::config::DivergenceConfig;
use vw_core::{DivergenceEvent, InstrumentId};

/// Minimum spread widening that overrides the debounce window (spec §7.2).
const WIDEN_STEP: Decimal = Decimal::from_parts(1, 0, 0, false, 2); // 0.01

#[derive(Debug, Clone, Copy)]
struct LastEmission {
    at: DateTime<Utc>,
    spread: Decimal,
}

/// Stateful per-match debouncer + threshold check. All methods take `&self`;
/// state lives in a DashMap keyed by match id.
#[derive(Debug)]
pub struct DivergenceDetector {
    cfg: DivergenceConfig,
    last_emission: DashMap<String, LastEmission>,
}

impl DivergenceDetector {
    pub fn new(cfg: DivergenceConfig) -> Self {
        Self {
            cfg,
            last_emission: DashMap::new(),
        }
    }

    /// Evaluate an updated match view at time `now`. Returns an event iff the
    /// max pairwise spread across fresh legs crosses the threshold and the
    /// debounce rules allow emission. Synchronous, hot-path safe.
    pub fn on_view(&self, view: &MatchView, now: DateTime<Utc>) -> Option<DivergenceEvent> {
        let freshness = Duration::seconds(self.cfg.freshness_secs as i64);
        let fresh_legs: Vec<(InstrumentId, Decimal)> = view
            .legs
            .iter()
            .filter_map(|leg| {
                let mid = leg.mid?;
                let last = leg.last_update?;
                (now - last < freshness).then(|| (leg.instrument.clone(), mid))
            })
            .collect();
        if fresh_legs.len() < 2 {
            return None;
        }

        // Max pairwise spread == max(mid) - min(mid); report all fresh legs.
        let mut min = fresh_legs[0].1;
        let mut max = fresh_legs[0].1;
        for (_, mid) in &fresh_legs[1..] {
            min = min.min(*mid);
            max = max.max(*mid);
        }
        let spread = max - min;
        if spread < self.cfg.threshold {
            return None;
        }

        let debounce = Duration::seconds(self.cfg.debounce_secs as i64);
        let mut entry = self
            .last_emission
            .entry(view.match_id.clone())
            .or_insert(LastEmission {
                at: DateTime::<Utc>::MIN_UTC,
                spread: Decimal::ZERO,
            });
        let prev = *entry;
        let within_window = prev.at > DateTime::<Utc>::MIN_UTC && now - prev.at < debounce;
        if within_window && spread - prev.spread < WIDEN_STEP {
            return None;
        }
        *entry = LastEmission { at: now, spread };
        drop(entry);

        Some(DivergenceEvent {
            match_id: view.match_id.clone(),
            spread,
            legs: fresh_legs,
            detected_at: now,
        })
    }

    /// Drop debounce state for matches not in the active set (used on match
    /// refresh so removed matches don't leak entries).
    pub fn retain_matches<F: Fn(&str) -> bool>(&self, keep: F) {
        self.last_emission.retain(|id, _| keep(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matchview::LegView;
    use rust_decimal_macros::dec;

    fn cfg() -> DivergenceConfig {
        DivergenceConfig::default() // threshold 0.03, freshness 60s, debounce 10s
    }

    fn leg(instrument: &str, mid: Decimal, last_update: DateTime<Utc>) -> LegView {
        LegView {
            instrument: InstrumentId(instrument.to_string()),
            yes_bid: Some(mid),
            yes_ask: Some(mid),
            mid: Some(mid),
            last_update: Some(last_update),
        }
    }

    fn view(match_id: &str, legs: Vec<LegView>) -> MatchView {
        MatchView {
            match_id: match_id.to_string(),
            legs,
            best_yes_bid: None,
            best_yes_ask: None,
        }
    }

    fn t0() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-18T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn below_threshold_is_silent() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), now), leg("b", dec!(0.521), now)],
        );
        assert!(d.on_view(&v, now).is_none());
    }

    #[test]
    fn at_threshold_emits_with_exact_spread_and_legs() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), now), leg("b", dec!(0.53), now)],
        );
        let ev = d.on_view(&v, now).expect("threshold is inclusive");
        assert_eq!(ev.match_id, "m");
        assert_eq!(ev.spread, dec!(0.03));
        assert_eq!(ev.detected_at, now);
        assert_eq!(
            ev.legs,
            vec![
                (InstrumentId("a".into()), dec!(0.50)),
                (InstrumentId("b".into()), dec!(0.53)),
            ]
        );
    }

    #[test]
    fn stale_leg_suppresses() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        // leg b is exactly 60s old: "fresher than 60s" is strict, so stale
        let v = view(
            "m",
            vec![
                leg("a", dec!(0.50), now),
                leg("b", dec!(0.60), now - Duration::seconds(60)),
            ],
        );
        assert!(d.on_view(&v, now).is_none());

        // just under 60s is fresh
        let v = view(
            "m",
            vec![
                leg("a", dec!(0.50), now),
                leg("b", dec!(0.60), now - Duration::seconds(59)),
            ],
        );
        assert!(d.on_view(&v, now).is_some());
    }

    #[test]
    fn missing_mid_or_never_updated_leg_suppresses() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let mut no_mid = leg("b", dec!(0.60), now);
        no_mid.mid = None;
        let v = view("m", vec![leg("a", dec!(0.50), now), no_mid]);
        assert!(d.on_view(&v, now).is_none());

        let mut never = leg("b", dec!(0.60), now);
        never.last_update = None;
        let v = view("m", vec![leg("a", dec!(0.50), now), never]);
        assert!(d.on_view(&v, now).is_none());
    }

    #[test]
    fn debounce_suppresses_within_window() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), now), leg("b", dec!(0.55), now)],
        );
        assert!(d.on_view(&v, now).is_some());
        // same spread 5s later: suppressed
        let later = now + Duration::seconds(5);
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), later), leg("b", dec!(0.55), later)],
        );
        assert!(d.on_view(&v, later).is_none());
        // window elapsed (10s): allowed again
        let after = now + Duration::seconds(10);
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), after), leg("b", dec!(0.55), after)],
        );
        assert!(d.on_view(&v, after).is_some());
    }

    #[test]
    fn widening_by_a_cent_overrides_debounce() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), now), leg("b", dec!(0.55), now)],
        );
        assert_eq!(d.on_view(&v, now).unwrap().spread, dec!(0.05));

        // +0.009 within the window: not enough
        let t1 = now + Duration::seconds(2);
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), t1), leg("b", dec!(0.559), t1)],
        );
        assert!(d.on_view(&v, t1).is_none());

        // +0.01 vs the last *emitted* spread: emits and restarts the window
        let t2 = now + Duration::seconds(4);
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), t2), leg("b", dec!(0.56), t2)],
        );
        assert_eq!(d.on_view(&v, t2).unwrap().spread, dec!(0.06));

        // narrowing never re-emits within the window
        let t3 = now + Duration::seconds(6);
        let v = view(
            "m",
            vec![leg("a", dec!(0.50), t3), leg("b", dec!(0.54), t3)],
        );
        assert!(d.on_view(&v, t3).is_none());
    }

    #[test]
    fn debounce_is_per_match() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let v1 = view(
            "m1",
            vec![leg("a", dec!(0.50), now), leg("b", dec!(0.55), now)],
        );
        let v2 = view(
            "m2",
            vec![leg("c", dec!(0.10), now), leg("d", dec!(0.15), now)],
        );
        assert!(d.on_view(&v1, now).is_some());
        // a different match is not debounced by m1's emission
        assert!(d.on_view(&v2, now + Duration::seconds(1)).is_some());
    }

    #[test]
    fn multi_leg_uses_max_pairwise_spread_and_reports_fresh_legs() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        // mids 0.50, 0.52, 0.56 → max pairwise = 0.06; stale leg excluded
        let v = view(
            "tri",
            vec![
                leg("a", dec!(0.50), now),
                leg("b", dec!(0.52), now),
                leg("c", dec!(0.56), now),
                leg("stale", dec!(0.99), now - Duration::seconds(3600)),
            ],
        );
        let ev = d.on_view(&v, now).unwrap();
        assert_eq!(ev.spread, dec!(0.06));
        assert_eq!(ev.legs.len(), 3);
        assert!(ev.legs.iter().all(|(id, _)| id.0 != "stale"));
    }

    #[test]
    fn retain_matches_drops_stale_debounce_state() {
        let d = DivergenceDetector::new(cfg());
        let now = t0();
        let v = view(
            "gone",
            vec![leg("a", dec!(0.50), now), leg("b", dec!(0.55), now)],
        );
        assert!(d.on_view(&v, now).is_some());
        d.retain_matches(|id| id != "gone");
        // debounce record was dropped, so the same spread emits immediately
        let t1 = now + Duration::seconds(1);
        let v = view(
            "gone",
            vec![leg("a", dec!(0.50), t1), leg("b", dec!(0.55), t1)],
        );
        assert!(d.on_view(&v, t1).is_some());
    }
}
