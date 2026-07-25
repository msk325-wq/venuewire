//! Similarity pass: candidate generation, lexical scoring, and optional LLM
//! adjudication via the Anthropic Messages API.
//!
//! Without the LLM, lexical scoring promotes to `Review` at most — token
//! overlap can say "these talk about the same thing" but not "these resolve
//! identically", so `High` is reserved for an adjudicated verdict.

use crate::rules::{normalize, NormalizedTitle};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;
use vw_core::Instrument;

/// A shortlisted cross-venue candidate pair (indices into the instrument
/// slice handed to [`candidate_pairs`]).
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredPair {
    pub a: usize,
    pub b: usize,
    pub score: f64,
}

fn jaccard(a: &BTreeSet<String>, b: &BTreeSet<String>) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 0.0;
    }
    let inter = a.intersection(b).count();
    let union = a.len() + b.len() - inter;
    inter as f64 / union as f64
}

/// True when two normalized titles carry mutually exclusive anchors:
/// both have thresholds (or dates) and the sets share nothing. "above 4.25%"
/// vs "above 4.50%" is the canonical near-miss this suppresses.
pub fn conflicting_anchors(a: &NormalizedTitle, b: &NormalizedTitle) -> bool {
    let thresholds_conflict = !a.thresholds.is_empty()
        && !b.thresholds.is_empty()
        && a.thresholds.is_disjoint(&b.thresholds);
    let dates_conflict =
        !a.dates.is_empty() && !b.dates.is_empty() && a.dates.is_disjoint(&b.dates);
    thresholds_conflict || dates_conflict
}

/// Token-set Jaccard over normalized title (and, when both sides have one,
/// description) signal. Range [0, 1].
pub fn lexical_score(a: &Instrument, b: &Instrument) -> f64 {
    let title = jaccard(
        &normalize(&a.title).combined(),
        &normalize(&b.title).combined(),
    );
    match (&a.description, &b.description) {
        (Some(da), Some(db)) => {
            let desc = jaccard(&normalize(da).combined(), &normalize(db).combined());
            0.7 * title + 0.3 * desc
        }
        _ => title,
    }
}

/// Generate scored cross-venue candidate pairs.
///
/// - Pairs must be from different venues.
/// - Close times, when both present, must lie within `window_hours` of each
///   other; a missing close time keeps the pair as a candidate but multiplies
///   the score by `missing_close_penalty` (< 1).
/// - Pairs with conflicting numeric/date anchors are dropped outright
///   (same event, different strike must never be matched automatically).
pub fn candidate_pairs(
    instruments: &[&Instrument],
    window_hours: i64,
    missing_close_penalty: f64,
) -> Vec<ScoredPair> {
    let norms: Vec<NormalizedTitle> = instruments.iter().map(|i| normalize(&i.title)).collect();
    let mut out = Vec::new();
    for i in 0..instruments.len() {
        for j in (i + 1)..instruments.len() {
            let (a, b) = (instruments[i], instruments[j]);
            if a.venue == b.venue {
                continue;
            }
            let penalty = match (a.close_time, b.close_time) {
                (Some(ta), Some(tb)) => {
                    if (ta - tb).num_seconds().abs() > window_hours * 3600 {
                        continue;
                    }
                    1.0
                }
                _ => missing_close_penalty,
            };
            if conflicting_anchors(&norms[i], &norms[j]) {
                continue;
            }
            let score = lexical_score(a, b) * penalty;
            if score > 0.0 {
                out.push(ScoredPair { a: i, b: j, score });
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// LLM adjudication (Anthropic Messages API, feature-gated by config + env)
// ---------------------------------------------------------------------------

/// One adjudicated verdict, in pair order.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Verdict {
    pub index: usize,
    pub same: bool,
    pub confidence: f64,
    #[serde(default)]
    pub reason: String,
}

fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

/// Build the batched adjudication prompt. Pure function — unit-tested with no
/// network access.
pub fn build_prompt(pairs: &[(&Instrument, &Instrument)]) -> String {
    let mut p = String::from(
        "You compare prediction markets listed on different venues. For each numbered pair \
         below, decide whether the two markets resolve on the SAME real-world event with the \
         SAME resolution criteria (same threshold, same date, same direction). Different \
         strike levels or different resolution dates mean NOT the same.\n\n\
         Respond with ONLY a JSON array, one object per pair, in order:\n\
         [{\"index\": 0, \"same\": true, \"confidence\": 0.95, \"reason\": \"one short sentence\"}]\n\
         \"confidence\" is a number from 0 to 1. No other text.\n",
    );
    for (idx, (a, b)) in pairs.iter().enumerate() {
        p.push_str(&format!("\nPair {idx}:\n"));
        for (label, inst) in [("A", a), ("B", b)] {
            p.push_str(&format!(
                "  {label} [{}] title: {}\n",
                inst.venue, inst.title
            ));
            if let Some(desc) = &inst.description {
                p.push_str(&format!(
                    "  {label} description: {}\n",
                    truncate_chars(desc, 600)
                ));
            }
            if let Some(close) = inst.close_time {
                p.push_str(&format!("  {label} closes: {}\n", close.to_rfc3339()));
            }
        }
    }
    p
}

/// Parse the model's reply (possibly fenced) into verdicts. Pure function —
/// unit-tested against canned JSON.
pub fn parse_verdicts(text: &str, expected: usize) -> anyhow::Result<Vec<Verdict>> {
    let start = text
        .find('[')
        .ok_or_else(|| anyhow::anyhow!("no JSON array in adjudication reply: {text:.120}"))?;
    let end = text
        .rfind(']')
        .ok_or_else(|| anyhow::anyhow!("unterminated JSON array in adjudication reply"))?;
    if end < start {
        anyhow::bail!("malformed JSON array in adjudication reply");
    }
    let mut verdicts: Vec<Verdict> = serde_json::from_str(&text[start..=end])?;
    verdicts.sort_by_key(|v| v.index);
    if verdicts.len() != expected || verdicts.iter().enumerate().any(|(i, v)| v.index != i) {
        anyhow::bail!(
            "adjudication reply has wrong pair coverage: expected {expected} verdicts \
             indexed 0..{expected}, got {:?}",
            verdicts.iter().map(|v| v.index).collect::<Vec<_>>()
        );
    }
    for v in &mut verdicts {
        v.confidence = v.confidence.clamp(0.0, 1.0);
    }
    Ok(verdicts)
}

/// Blocking Anthropic Messages API client for pair adjudication.
pub struct Adjudicator {
    client: reqwest::blocking::Client,
    api_key: String,
    pub api_url: String,
    pub model: String,
}

impl fmt::Debug for Adjudicator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Adjudicator")
            .field("api_url", &self.api_url)
            .field("model", &self.model)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl Adjudicator {
    /// Construct from `ANTHROPIC_API_KEY`; `None` when the key is absent
    /// (the engine then runs lexical-only with the `Review` ceiling).
    pub fn from_env(model: &str, api_url: &str) -> Option<Adjudicator> {
        let api_key = std::env::var("ANTHROPIC_API_KEY").ok()?;
        if api_key.is_empty() {
            return None;
        }
        Some(Adjudicator {
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("reqwest client"),
            api_key,
            api_url: api_url.to_owned(),
            model: model.to_owned(),
        })
    }

    /// Adjudicate one batch of pairs. Returns verdicts in pair order.
    pub fn adjudicate(&self, pairs: &[(&Instrument, &Instrument)]) -> anyhow::Result<Vec<Verdict>> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        let prompt = build_prompt(pairs);
        let max_tokens = (pairs.len() * 160).clamp(512, 8192);
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        });
        let resp = self
            .client
            .post(&self.api_url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()?;
        let status = resp.status();
        let payload: serde_json::Value = resp.json()?;
        if !status.is_success() {
            anyhow::bail!("anthropic api error {status}: {payload}");
        }
        let text = payload
            .get("content")
            .and_then(|c| c.as_array())
            .and_then(|blocks| {
                blocks
                    .iter()
                    .find_map(|b| (b.get("type")?.as_str()? == "text").then(|| b.get("text"))?)
            })
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("no text block in anthropic response: {payload}"))?;
        parse_verdicts(text, pairs.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use vw_core::{InstrumentId, Venue};

    fn inst(
        venue: Venue,
        native: &str,
        title: &str,
        desc: Option<&str>,
        close: Option<&str>,
    ) -> Instrument {
        Instrument {
            id: InstrumentId::new(venue, native),
            venue,
            title: title.to_owned(),
            description: desc.map(str::to_owned),
            close_time: close.map(|c| {
                chrono::DateTime::parse_from_rfc3339(c)
                    .unwrap()
                    .with_timezone(&Utc)
            }),
            category: None,
            raw: serde_json::Value::Null,
        }
    }

    /// Real Kalshi Fed phrasing vs a differently-phrased Polymarket question:
    /// no rule-key collision, but lexically similar enough for Review.
    fn fed_pair() -> (Instrument, Instrument) {
        (
            inst(
                Venue::Kalshi,
                "KXFED-27APR-T4.25",
                "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
                None,
                Some("2027-04-28T18:00:00Z"),
            ),
            inst(
                Venue::Polymarket,
                "0xfed425",
                "Will the Fed keep the federal funds rate above 4.25% after its April 2027 meeting?",
                None,
                Some("2027-04-28T23:59:00Z"),
            ),
        )
    }

    #[test]
    fn fed_pair_scores_into_review_band() {
        let (k, p) = fed_pair();
        let score = lexical_score(&k, &p);
        assert!(
            score > 0.55,
            "score {score} should clear the review threshold"
        );
        assert!(score < 0.9, "lexical score should not look like certainty");
    }

    #[test]
    fn candidate_pairs_respect_close_window_and_venue() {
        let (k, p) = fed_pair();
        let pairs = candidate_pairs(&[&k, &p], 48, 0.85);
        assert_eq!(pairs.len(), 1);
        assert!((pairs[0].score - lexical_score(&k, &p)).abs() < 1e-9);

        // Same-venue pair: never a candidate.
        let k2 = inst(
            Venue::Kalshi,
            "K2",
            &p.title,
            None,
            Some("2027-04-28T18:00:00Z"),
        );
        assert!(candidate_pairs(&[&k, &k2], 48, 0.85).is_empty());

        // Outside ±48h: dropped.
        let far = inst(
            Venue::Polymarket,
            "0xfar",
            &p.title,
            None,
            Some("2027-05-15T00:00:00Z"),
        );
        assert!(candidate_pairs(&[&k, &far], 48, 0.85).is_empty());
    }

    #[test]
    fn missing_close_time_is_still_a_candidate_scored_lower() {
        let (k, mut p) = fed_pair();
        let with_close = candidate_pairs(&[&k, &p], 48, 0.85)[0].score;
        p.close_time = None;
        let pairs = candidate_pairs(&[&k, &p], 48, 0.85);
        assert_eq!(pairs.len(), 1, "missing close_time must remain a candidate");
        assert!(
            pairs[0].score < with_close,
            "penalized {} !< {}",
            pairs[0].score,
            with_close
        );
    }

    #[test]
    fn conflicting_thresholds_suppress_the_pair() {
        // Same Fed event, different strike — near-miss must not be matched.
        let k = inst(
            Venue::Kalshi,
            "KXFED-27APR-T4.25",
            "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
            None,
            Some("2027-04-28T18:00:00Z"),
        );
        let p = inst(
            Venue::Polymarket,
            "0xfed450",
            "Will the Fed keep the federal funds rate above 4.50% after its April 2027 meeting?",
            None,
            Some("2027-04-28T18:00:00Z"),
        );
        assert!(candidate_pairs(&[&k, &p], 48, 0.85).is_empty());
    }

    #[test]
    fn conflicting_dates_suppress_the_pair() {
        let k = inst(
            Venue::Kalshi,
            "K",
            "Will CPI rise more than 0.1% in July 2026?",
            None,
            None,
        );
        let p = inst(
            Venue::Polymarket,
            "P",
            "Will CPI rise more than 0.1% in August 2026?",
            None,
            None,
        );
        assert!(candidate_pairs(&[&k, &p], 48, 0.85).is_empty());
    }

    #[test]
    fn descriptions_participate_in_scoring() {
        let (mut k, mut p) = fed_pair();
        k.description = Some("Resolves YES if the upper bound of the target federal funds rate is above 4.25% after the April 2027 FOMC meeting.".into());
        p.description = Some("This market resolves to Yes if the federal funds target rate upper bound exceeds 4.25% following the April 2027 FOMC meeting.".into());
        let with_desc = lexical_score(&k, &p);
        k.description = None;
        p.description = None;
        let title_only = lexical_score(&k, &p);
        assert!(with_desc > title_only, "{with_desc} !> {title_only}");
    }

    #[test]
    fn prompt_contains_pairs_and_instructions() {
        let (k, p) = fed_pair();
        let prompt = build_prompt(&[(&k, &p)]);
        assert!(prompt.contains("Pair 0:"));
        assert!(prompt.contains(&k.title));
        assert!(prompt.contains(&p.title));
        assert!(prompt.contains("[kalshi]"));
        assert!(prompt.contains("[polymarket]"));
        assert!(prompt.contains("JSON array"));
        assert!(prompt.contains("\"confidence\""));
        // close times included
        assert!(prompt.contains("2027-04-28T18:00:00+00:00"));
    }

    #[test]
    fn prompt_truncates_long_descriptions() {
        let (mut k, p) = fed_pair();
        k.description = Some("x".repeat(5000));
        let prompt = build_prompt(&[(&k, &p)]);
        assert!(!prompt.contains(&"x".repeat(601)));
        assert!(prompt.contains(&"x".repeat(600)));
    }

    #[test]
    fn parse_verdicts_plain_array() {
        let text = r#"[{"index":0,"same":true,"confidence":0.95,"reason":"same FOMC strike"},
                       {"index":1,"same":false,"confidence":0.2,"reason":"different threshold"}]"#;
        let v = parse_verdicts(text, 2).unwrap();
        assert_eq!(v.len(), 2);
        assert!(v[0].same);
        assert!((v[0].confidence - 0.95).abs() < 1e-9);
        assert!(!v[1].same);
    }

    #[test]
    fn parse_verdicts_handles_fences_and_preamble() {
        let text = "Here are my verdicts:\n```json\n[{\"index\": 0, \"same\": true, \"confidence\": 0.9, \"reason\": \"ok\"}]\n```";
        let v = parse_verdicts(text, 1).unwrap();
        assert_eq!(v.len(), 1);
        assert!(v[0].same);
    }

    #[test]
    fn parse_verdicts_sorts_and_clamps() {
        let text = r#"[{"index":1,"same":false,"confidence":-0.4,"reason":""},
                       {"index":0,"same":true,"confidence":1.7,"reason":""}]"#;
        let v = parse_verdicts(text, 2).unwrap();
        assert_eq!(v[0].index, 0);
        assert!((v[0].confidence - 1.0).abs() < 1e-9);
        assert!((v[1].confidence - 0.0).abs() < 1e-9);
    }

    #[test]
    fn parse_verdicts_rejects_wrong_coverage() {
        assert!(parse_verdicts("[]", 1).is_err());
        assert!(parse_verdicts(r#"[{"index":0,"same":true,"confidence":0.9}]"#, 2).is_err());
        assert!(parse_verdicts(r#"[{"index":3,"same":true,"confidence":0.9}]"#, 1).is_err());
        assert!(parse_verdicts("no json here", 1).is_err());
    }

    /// Live smoke test against the real API. Never runs in CI:
    /// `cargo test -p vw-matcher -- --ignored llm_smoke` with
    /// ANTHROPIC_API_KEY set.
    #[test]
    #[ignore = "hits the live Anthropic API; requires ANTHROPIC_API_KEY"]
    fn llm_smoke() {
        let adj =
            Adjudicator::from_env("claude-haiku-4-5", "https://api.anthropic.com/v1/messages")
                .expect("ANTHROPIC_API_KEY must be set for the smoke test");
        let (k, p) = fed_pair();
        let verdicts = adj.adjudicate(&[(&k, &p)]).unwrap();
        assert_eq!(verdicts.len(), 1);
    }
}
