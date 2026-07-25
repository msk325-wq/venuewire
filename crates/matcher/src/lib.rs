//! Cross-venue market matching engine (M3).
//!
//! Pipeline per pass: rule pass (title normalization + exact key collisions →
//! `Exact`) → similarity pass (lexical shortlist, optional LLM adjudication →
//! `High`/`Review`) → merge into the human-editable YAML registry.
//!
//! Pure library: no background tasks, no globals. The daemon owns the
//! schedule and calls [`Matcher::run_pass`] at startup and on a timer, off
//! the hot tick path (e.g. via `spawn_blocking` — the LLM client is
//! deliberately synchronous).

pub mod registry;
pub mod rules;
pub mod similarity;

pub use registry::{MatchStatus, Registry, RegistryEntry};

use similarity::Adjudicator;
use std::collections::HashSet;
use std::path::PathBuf;
use vw_core::{Instrument, InstrumentId, MatchConfidence, MatchMethod, MatchedMarket};

/// Engine tuning knobs. Mirrors `vw_core::config::MatcherConfig` (the
/// `llm_adjudication` flag) without depending on config plumbing; everything
/// else has conservative defaults.
#[derive(Debug, Clone)]
pub struct MatcherOptions {
    /// Enable LLM adjudication of shortlisted pairs. Also requires
    /// `ANTHROPIC_API_KEY` in the environment at runtime; without either,
    /// the similarity pass is lexical-only and promotes to `Review` at most.
    pub llm_adjudication: bool,
    /// Candidate pairs must have close times within this window of each
    /// other (when both are known).
    pub close_time_window_hours: i64,
    /// `Exact` rule matches additionally require close times (when both
    /// known) to agree within this tighter guard.
    pub exact_close_guard_hours: i64,
    /// Lexical score below which a pair is not even shortlisted for the LLM.
    pub shortlist_threshold: f64,
    /// Lexical score at or above which a pair becomes `Review` when the LLM
    /// is unavailable.
    pub review_threshold: f64,
    /// LLM confidence at or above which a `same: true` verdict is `High`.
    pub llm_high_threshold: f64,
    /// LLM confidence at or above which a `same: true` verdict is `Review`.
    pub llm_review_threshold: f64,
    /// Score multiplier applied when either side lacks a close time.
    pub missing_close_time_penalty: f64,
    /// Anthropic model id used for adjudication.
    pub llm_model: String,
    /// Anthropic Messages API endpoint (overridable for tests).
    pub anthropic_api_url: String,
    /// Pairs per adjudication request.
    pub llm_batch_size: usize,
}

impl Default for MatcherOptions {
    fn default() -> Self {
        Self {
            llm_adjudication: false,
            close_time_window_hours: 48,
            exact_close_guard_hours: 72,
            shortlist_threshold: 0.35,
            review_threshold: 0.55,
            llm_high_threshold: 0.9,
            llm_review_threshold: 0.6,
            missing_close_time_penalty: 0.85,
            llm_model: "claude-haiku-4-5".to_owned(),
            anthropic_api_url: "https://api.anthropic.com/v1/messages".to_owned(),
            llm_batch_size: 16,
        }
    }
}

/// What one pass did to the registry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MatchReport {
    /// Entries appended this pass.
    pub new_matches: usize,
    /// `review` entries promoted to `active` by a stronger finding.
    pub promoted: usize,
    /// Candidates suppressed by the registry (rejected leg sets, manual
    /// entries, or already-present entries with no upgrade).
    pub skipped: usize,
}

/// The matching engine facade.
#[derive(Debug)]
pub struct Matcher {
    options: MatcherOptions,
    registry_path: PathBuf,
}

fn confidence_rank(c: MatchConfidence) -> u8 {
    match c {
        MatchConfidence::Exact => 2,
        MatchConfidence::High => 1,
        MatchConfidence::Review => 0,
    }
}

fn status_for(confidence: MatchConfidence) -> MatchStatus {
    match confidence {
        MatchConfidence::Exact | MatchConfidence::High => MatchStatus::Active,
        MatchConfidence::Review => MatchStatus::Review,
    }
}

impl Matcher {
    pub fn new(options: MatcherOptions, registry_path: impl Into<PathBuf>) -> Matcher {
        Matcher {
            options,
            registry_path: registry_path.into(),
        }
    }

    /// Run one matching pass over the current instrument universe.
    ///
    /// Reloads the registry (picking up human edits), runs the rule pass and
    /// the similarity pass over instruments not already pinned by an active
    /// entry, merges the findings, and writes the registry back atomically.
    pub fn run_pass(&mut self, instruments: &[Instrument]) -> anyhow::Result<MatchReport> {
        let mut reg = Registry::load(&self.registry_path)?;
        let mut report = MatchReport::default();

        // Instruments already claimed by an active entry (manual or engine)
        // are settled; review/rejected legs stay eligible so a stronger
        // finding can promote, and rejection of one pairing doesn't freeze
        // the instruments out of other pairings.
        let pinned: HashSet<InstrumentId> = reg
            .entries
            .iter()
            .filter(|e| e.status == MatchStatus::Active)
            .flat_map(|e| e.legs.iter().cloned())
            .collect();
        let eligible: Vec<&Instrument> = instruments
            .iter()
            .filter(|i| !pinned.contains(&i.id))
            .collect();

        // --- Rule pass -----------------------------------------------------
        let mut consumed: HashSet<InstrumentId> = HashSet::new();
        for m in rules::rule_pass(&eligible, self.options.exact_close_guard_hours) {
            // A key collision is decisive either way: don't re-offer these
            // instruments to the similarity pass this run.
            consumed.extend(m.legs.iter().cloned());
            self.merge(&mut reg, m, None, &mut report);
        }

        // --- Similarity pass -------------------------------------------------
        let remaining: Vec<&Instrument> = eligible
            .iter()
            .filter(|i| !consumed.contains(&i.id))
            .copied()
            .collect();
        let mut pairs = similarity::candidate_pairs(
            &remaining,
            self.options.close_time_window_hours,
            self.options.missing_close_time_penalty,
        );
        pairs.retain(|p| p.score >= self.options.shortlist_threshold);
        // Greedy one-match-per-instrument: best score first.
        pairs.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut taken: HashSet<usize> = HashSet::new();
        let selected: Vec<similarity::ScoredPair> = pairs
            .into_iter()
            .filter(|p| {
                if taken.contains(&p.a) || taken.contains(&p.b) {
                    return false;
                }
                taken.insert(p.a);
                taken.insert(p.b);
                true
            })
            .collect();

        let adjudicator = if self.options.llm_adjudication {
            let adj =
                Adjudicator::from_env(&self.options.llm_model, &self.options.anthropic_api_url);
            if adj.is_none() {
                tracing::warn!(
                    "llm_adjudication enabled but ANTHROPIC_API_KEY is not set; \
                     falling back to lexical-only scoring (Review ceiling)"
                );
            }
            adj
        } else {
            None
        };

        match &adjudicator {
            Some(adj) => {
                for chunk in selected.chunks(self.options.llm_batch_size.max(1)) {
                    let pair_refs: Vec<(&Instrument, &Instrument)> = chunk
                        .iter()
                        .map(|p| (remaining[p.a], remaining[p.b]))
                        .collect();
                    match adj.adjudicate(&pair_refs) {
                        Ok(verdicts) => {
                            for (p, v) in chunk.iter().zip(verdicts) {
                                let confidence =
                                    if v.same && v.confidence >= self.options.llm_high_threshold {
                                        MatchConfidence::High
                                    } else if v.same
                                        && v.confidence >= self.options.llm_review_threshold
                                    {
                                        MatchConfidence::Review
                                    } else {
                                        continue;
                                    };
                                let note = format!("llm {:.2}: {}", v.confidence, v.reason.trim());
                                let m = pair_match(remaining[p.a], remaining[p.b], confidence);
                                self.merge(&mut reg, m, Some(note), &mut report);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "llm adjudication failed; falling back to lexical-only for this batch");
                            self.merge_lexical(&mut reg, chunk, &remaining, &mut report);
                        }
                    }
                }
            }
            None => self.merge_lexical(&mut reg, &selected, &remaining, &mut report),
        }

        reg.save(&self.registry_path)?;
        tracing::info!(
            new = report.new_matches,
            promoted = report.promoted,
            skipped = report.skipped,
            "matching pass complete"
        );
        Ok(report)
    }

    /// The trusted matches currently in the registry (for downstream wiring).
    pub fn active_matches(&self) -> anyhow::Result<Vec<MatchedMarket>> {
        Ok(Registry::load(&self.registry_path)?.active_matches())
    }

    /// Lexical-only outcome: `Review` at most, never `High`. Token overlap
    /// can flag "probably the same" for a human queue, but is not evidence
    /// of identical resolution criteria.
    fn merge_lexical(
        &self,
        reg: &mut Registry,
        pairs: &[similarity::ScoredPair],
        instruments: &[&Instrument],
        report: &mut MatchReport,
    ) {
        for p in pairs {
            if p.score < self.options.review_threshold {
                continue;
            }
            let m = pair_match(instruments[p.a], instruments[p.b], MatchConfidence::Review);
            self.merge(
                reg,
                m,
                Some(format!("lexical score {:.2}", p.score)),
                report,
            );
        }
    }

    /// Merge one candidate into the registry, honoring override semantics.
    fn merge(
        &self,
        reg: &mut Registry,
        matched: MatchedMarket,
        note: Option<String>,
        report: &mut MatchReport,
    ) {
        let key = rules::legs_key(&matched.legs);
        match reg.find_by_legs(&key) {
            Some(idx) => {
                let entry = &mut reg.entries[idx];
                if entry.status == MatchStatus::Rejected {
                    tracing::debug!(match_id = %entry.match_id, "leg set rejected in registry; not re-emitting");
                    report.skipped += 1;
                } else if entry.method == MatchMethod::Manual {
                    // Manual entries override everything, including their
                    // confidence/status; the engine never touches them.
                    report.skipped += 1;
                } else if entry.status == MatchStatus::Review
                    && confidence_rank(matched.confidence) > confidence_rank(entry.confidence)
                {
                    entry.status = status_for(matched.confidence);
                    entry.confidence = matched.confidence;
                    entry.method = matched.method;
                    if let Some(n) = note {
                        entry.note = Some(n);
                    }
                    report.promoted += 1;
                } else {
                    report.skipped += 1;
                }
            }
            None => {
                reg.entries.push(RegistryEntry {
                    match_id: matched.match_id,
                    status: status_for(matched.confidence),
                    legs: matched.legs,
                    confidence: matched.confidence,
                    method: matched.method,
                    note,
                });
                report.new_matches += 1;
            }
        }
    }
}

fn pair_match(a: &Instrument, b: &Instrument, confidence: MatchConfidence) -> MatchedMarket {
    let mut legs = vec![a.id.clone(), b.id.clone()];
    legs.sort_by(|x, y| x.0.cmp(&y.0));
    MatchedMarket {
        match_id: rules::make_match_id(&[a, b]),
        legs,
        confidence,
        method: MatchMethod::Embedding,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use vw_core::Venue;

    fn inst(venue: Venue, native: &str, title: &str, close: Option<&str>) -> Instrument {
        Instrument {
            id: InstrumentId::new(venue, native),
            venue,
            title: title.to_owned(),
            description: None,
            close_time: close.map(|c| {
                chrono::DateTime::parse_from_rfc3339(c)
                    .unwrap()
                    .with_timezone(&Utc)
            }),
            category: None,
            raw: serde_json::Value::Null,
        }
    }

    fn cpi_pair() -> (Instrument, Instrument) {
        (
            inst(
                Venue::Kalshi,
                "KXCPI-26JUL-T0.0",
                "Will CPI rise more than 0.0% in July 2026?",
                Some("2026-08-12T12:00:00Z"),
            ),
            inst(
                Venue::Polymarket,
                "0xcpi26jul",
                "Will CPI rise more than 0.0% in Jul 2026",
                Some("2026-08-12T00:00:00Z"),
            ),
        )
    }

    fn fed_pair() -> (Instrument, Instrument) {
        (
            inst(
                Venue::Kalshi,
                "KXFED-27APR-T4.25",
                "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
                Some("2027-04-28T18:00:00Z"),
            ),
            inst(
                Venue::Polymarket,
                "0xfed425",
                "Will the Fed keep the federal funds rate above 4.25% after its April 2027 meeting?",
                Some("2027-04-28T23:59:00Z"),
            ),
        )
    }

    fn matcher_at(dir: &tempfile::TempDir) -> Matcher {
        Matcher::new(MatcherOptions::default(), dir.path().join("matches.yaml"))
    }

    #[test]
    fn cpi_pair_becomes_exact_active_via_rules() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = matcher_at(&dir);
        let (k, p) = cpi_pair();
        let report = m.run_pass(&[k, p]).unwrap();
        assert_eq!(
            report,
            MatchReport {
                new_matches: 1,
                promoted: 0,
                skipped: 0
            }
        );

        let reg = Registry::load(&dir.path().join("matches.yaml")).unwrap();
        assert_eq!(reg.entries.len(), 1);
        let e = &reg.entries[0];
        assert_eq!(e.confidence, MatchConfidence::Exact);
        assert_eq!(e.method, MatchMethod::Rule);
        assert_eq!(e.status, MatchStatus::Active);
        assert_eq!(e.legs.len(), 2);
    }

    #[test]
    fn fed_pair_becomes_review_via_similarity_without_llm() {
        // Differently phrased across venues: no rule-key collision, so the
        // similarity pass catches it — and without an LLM the ceiling is
        // Review, never High.
        let dir = tempfile::tempdir().unwrap();
        let mut m = matcher_at(&dir);
        let (k, p) = fed_pair();
        let report = m.run_pass(&[k, p]).unwrap();
        assert_eq!(report.new_matches, 1);

        let reg = Registry::load(&dir.path().join("matches.yaml")).unwrap();
        let e = &reg.entries[0];
        assert_eq!(e.confidence, MatchConfidence::Review);
        assert_eq!(e.method, MatchMethod::Embedding);
        assert_eq!(e.status, MatchStatus::Review);
        assert!(e.note.as_deref().unwrap_or("").starts_with("lexical score"));
    }

    #[test]
    fn near_miss_different_threshold_matches_nothing() {
        // Same Fed event, strikes 4.25% vs 4.50%: must not match, above all
        // not Exact.
        let dir = tempfile::tempdir().unwrap();
        let mut m = matcher_at(&dir);
        let k = inst(
            Venue::Kalshi,
            "KXFED-27APR-T4.25",
            "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
            Some("2027-04-28T18:00:00Z"),
        );
        let p = inst(
            Venue::Polymarket,
            "0xfed450",
            "Will the Fed keep the federal funds rate above 4.50% after its April 2027 meeting?",
            Some("2027-04-28T23:59:00Z"),
        );
        let report = m.run_pass(&[k, p]).unwrap();
        assert_eq!(report, MatchReport::default());
        let reg = Registry::load(&dir.path().join("matches.yaml")).unwrap();
        assert!(reg.entries.is_empty());
    }

    #[test]
    fn manual_entry_overrides_the_engine() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("matches.yaml");
        let (k, p) = cpi_pair();
        // A human pinned this pair manually (with their own confidence).
        let manual = RegistryEntry {
            match_id: "cpi-manual".into(),
            legs: vec![k.id.clone(), p.id.clone()],
            confidence: MatchConfidence::High,
            method: MatchMethod::Manual,
            status: MatchStatus::Active,
            note: Some("verified by hand".into()),
        };
        Registry {
            entries: vec![manual.clone()],
        }
        .save(&path)
        .unwrap();

        let mut m = Matcher::new(MatcherOptions::default(), &path);
        let report = m.run_pass(&[k, p]).unwrap();
        // Active legs are pinned: the engine doesn't even re-derive the pair.
        assert_eq!(report, MatchReport::default());
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.entries, vec![manual]);
    }

    #[test]
    fn manual_review_entry_is_never_modified() {
        // Even when the engine would promote (rule pass says Exact), a
        // manual entry sitting in review belongs to the human.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("matches.yaml");
        let (k, p) = cpi_pair();
        let manual = RegistryEntry {
            match_id: "cpi-manual-review".into(),
            legs: vec![k.id.clone(), p.id.clone()],
            confidence: MatchConfidence::Review,
            method: MatchMethod::Manual,
            status: MatchStatus::Review,
            note: None,
        };
        Registry {
            entries: vec![manual.clone()],
        }
        .save(&path)
        .unwrap();

        let mut m = Matcher::new(MatcherOptions::default(), &path);
        let report = m.run_pass(&[k, p]).unwrap();
        assert_eq!(
            report,
            MatchReport {
                new_matches: 0,
                promoted: 0,
                skipped: 1
            }
        );
        assert_eq!(Registry::load(&path).unwrap().entries, vec![manual]);
    }

    #[test]
    fn rejected_entry_is_never_re_emitted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("matches.yaml");
        let (k, p) = cpi_pair();
        let rejected = RegistryEntry {
            match_id: "cpi-rejected".into(),
            legs: vec![k.id.clone(), p.id.clone()],
            confidence: MatchConfidence::Exact,
            method: MatchMethod::Rule,
            status: MatchStatus::Rejected,
            note: Some("different settlement source".into()),
        };
        Registry {
            entries: vec![rejected.clone()],
        }
        .save(&path)
        .unwrap();

        let mut m = Matcher::new(MatcherOptions::default(), &path);
        // The rule pass re-derives the pair, but the registry blocks it.
        let report = m.run_pass(&[k.clone(), p.clone()]).unwrap();
        assert_eq!(
            report,
            MatchReport {
                new_matches: 0,
                promoted: 0,
                skipped: 1
            }
        );
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.entries, vec![rejected]);

        // And it stays blocked on every subsequent pass.
        let report = m.run_pass(&[k, p]).unwrap();
        assert_eq!(report.new_matches, 0);
    }

    #[test]
    fn review_entry_promotes_when_rules_find_exact() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("matches.yaml");
        let (k, p) = cpi_pair();
        // As if an earlier lexical-only pass had queued this for review.
        let review = RegistryEntry {
            match_id: "cpi-review".into(),
            legs: vec![k.id.clone(), p.id.clone()],
            confidence: MatchConfidence::Review,
            method: MatchMethod::Embedding,
            status: MatchStatus::Review,
            note: Some("lexical score 0.61".into()),
        };
        Registry {
            entries: vec![review],
        }
        .save(&path)
        .unwrap();

        let mut m = Matcher::new(MatcherOptions::default(), &path);
        let report = m.run_pass(&[k, p]).unwrap();
        assert_eq!(
            report,
            MatchReport {
                new_matches: 0,
                promoted: 1,
                skipped: 0
            }
        );
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.entries.len(), 1);
        assert_eq!(reg.entries[0].status, MatchStatus::Active);
        assert_eq!(reg.entries[0].confidence, MatchConfidence::Exact);
        assert_eq!(reg.entries[0].method, MatchMethod::Rule);
        // The stable match_id chosen at first write is preserved.
        assert_eq!(reg.entries[0].match_id, "cpi-review");
    }

    #[test]
    fn repeated_passes_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = matcher_at(&dir);
        let (k, p) = cpi_pair();
        let (fk, fp) = fed_pair();
        let universe = vec![k, p, fk, fp];
        let first = m.run_pass(&universe).unwrap();
        assert_eq!(first.new_matches, 2);

        let second = m.run_pass(&universe).unwrap();
        assert_eq!(second.new_matches, 0);
        assert_eq!(second.promoted, 0);
        // The Exact/active pair is pinned entirely; the review pair is
        // re-derived and skipped as already present.
        assert_eq!(second.skipped, 1);
        let reg = Registry::load(&dir.path().join("matches.yaml")).unwrap();
        assert_eq!(reg.entries.len(), 2);
    }

    #[test]
    fn active_matches_facade_reads_the_registry() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = matcher_at(&dir);
        let (k, p) = cpi_pair();
        m.run_pass(&[k, p]).unwrap();
        let active = m.active_matches().unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].confidence, MatchConfidence::Exact);
    }
}
