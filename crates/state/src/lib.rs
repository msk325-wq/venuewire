//! In-memory book state, per-match best-price views, Redis write-behind mirror,
//! and the divergence detector (M4, spec §7.1–§7.2).
//!
//! [`PipelineState`] is the facade the daemon wires between the connector
//! channel and the server: feed it normalized [`Tick`]s, get back
//! [`DivergenceEvent`]s, and read quote/match snapshots for the API layer.
//!
//! ## Hot-path guarantees
//!
//! [`PipelineState::apply`] is **synchronous** — it never awaits and never
//! holds a lock across an await point (there are none). Per tick it performs:
//!
//! 1. one injected-clock read;
//! 2. one DashMap entry upsert in [`BookState`] (rejecting out-of-order seq);
//! 3. an optional dirty marker insert on the [`MirrorHandle`] (no
//!    serialization, no I/O — the spawned flush task does that off-path, and a
//!    Redis outage only drops mirror writes, never stalls ingestion);
//! 4. for each match touching the instrument (usually 0 or 1): a brief
//!    `RwLock` read of the match index, a view recompute over that match's
//!    legs, and a divergence threshold/freshness/debounce check.
//!
//! Unmatched instruments pay only steps 1–3. Matcher refreshes
//! ([`PipelineState::set_matches`]) swap the match index and rebuild views
//! without touching book state, so ticks keep flowing during a refresh.

pub mod book;
pub mod clock;
pub mod divergence;
pub mod matchview;
pub mod mirror;

pub use book::{BookState, LatestQuote};
pub use clock::{Clock, ManualClock, SystemClock};
pub use divergence::DivergenceDetector;
pub use matchview::{LegView, MatchIndex, MatchView, MatchViews};
pub use mirror::{
    MirrorEntry, MirrorHandle, QuoteSink, RedisSink, SinkError, DEFAULT_FLUSH_INTERVAL,
    DEFAULT_TTL_SECS,
};

use std::sync::Arc;
use vw_core::config::DivergenceConfig;
use vw_core::{DivergenceEvent, InstrumentId, MatchedMarket, Tick};

/// Book state + match views + divergence detection behind one `&self` API.
#[derive(Debug)]
pub struct PipelineState {
    book: BookState,
    views: MatchViews,
    detector: DivergenceDetector,
    clock: Arc<dyn Clock>,
    mirror: Option<MirrorHandle>,
}

impl PipelineState {
    /// `matches` should already exclude review/rejected entries (the matcher
    /// filters them); matches with fewer than two legs are dropped here.
    /// Pass a [`MirrorHandle`] to mirror quotes/views to Redis (spawn its
    /// flush task separately via [`MirrorHandle::spawn`]), or `None` to run
    /// without a mirror.
    pub fn new(
        divergence_cfg: DivergenceConfig,
        matches: &[MatchedMarket],
        clock: Arc<dyn Clock>,
        mirror: Option<MirrorHandle>,
    ) -> Self {
        let book = BookState::new();
        let views = MatchViews::new(matches, &book);
        Self {
            book,
            views,
            detector: DivergenceDetector::new(divergence_cfg),
            clock,
            mirror,
        }
    }

    /// Apply one tick: book update → match-view recompute → divergence check.
    /// Returns the divergence events this tick produced (usually empty).
    /// Out-of-order ticks (see [`book`] module docs) are dropped and return
    /// no events. Synchronous; see the crate docs for hot-path guarantees.
    pub fn apply(&self, tick: &Tick) -> Vec<DivergenceEvent> {
        let now = self.clock.now();
        let Some(quote) = self.book.apply_tick(tick, now) else {
            return Vec::new();
        };
        if let Some(mirror) = &self.mirror {
            mirror.mark_quote(&tick.instrument, &quote);
        }
        let updated = self.views.on_tick(&tick.instrument, &self.book);
        let mut events = Vec::new();
        for view in &updated {
            if let Some(mirror) = &self.mirror {
                mirror.mark_match(view);
            }
            if let Some(event) = self.detector.on_view(view, now) {
                events.push(event);
            }
        }
        events
    }

    /// Swap in a refreshed match set (matcher timer). Book state is untouched;
    /// views are rebuilt from current quotes and stale debounce state for
    /// removed matches is dropped.
    pub fn set_matches(&self, matches: &[MatchedMarket]) {
        self.views.rebuild(matches, &self.book);
        let keep: std::collections::HashSet<String> = matches
            .iter()
            .filter(|m| m.legs.len() >= 2)
            .map(|m| m.match_id.clone())
            .collect();
        self.detector.retain_matches(|id| keep.contains(id));
    }

    // ---- snapshot accessors (M5 server surface) ----

    /// Latest quote for one instrument.
    pub fn quote(&self, instrument: &InstrumentId) -> Option<LatestQuote> {
        self.book.quote(instrument)
    }

    /// Latest quote for every instrument.
    pub fn all_quotes(&self) -> Vec<(InstrumentId, LatestQuote)> {
        self.book.all_quotes()
    }

    /// Live view of one match.
    pub fn match_view(&self, match_id: &str) -> Option<MatchView> {
        self.views.get(match_id)
    }

    /// Live views of every match.
    pub fn all_match_views(&self) -> Vec<MatchView> {
        self.views.all()
    }

    /// Number of instruments with at least one accepted tick.
    pub fn instrument_count(&self) -> usize {
        self.book.instrument_count()
    }

    /// Number of active (>= 2 leg) matches.
    pub fn match_count(&self) -> usize {
        self.views.match_count()
    }

    /// Ticks accepted / rejected-as-out-of-order since startup.
    pub fn tick_counts(&self) -> (u64, u64) {
        (self.book.accepted_count(), self.book.rejected_count())
    }

    /// Direct read access to the underlying book (metrics, debugging).
    pub fn book(&self) -> &BookState {
        &self.book
    }
}
