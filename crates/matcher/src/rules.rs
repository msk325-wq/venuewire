//! Rule pass: title normalization, entity/threshold/date extraction, and
//! exact matching on normalized-key collisions.
//!
//! The rule pass is deliberately conservative: a false `Exact` match poisons
//! divergence detection, so a key collision is only promoted to `Exact` when
//! the titles carry a numeric or date anchor, the collision is unambiguous
//! (exactly one instrument per venue), and the venues' close times agree.

use regex::{Captures, Regex};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::LazyLock;
use vw_core::{Instrument, InstrumentId, MatchConfidence, MatchMethod, MatchedMarket, Venue};

const MONTHS_ALT: &str = "jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?";

/// "28 April 2027", "28th Apr 2027" → 2027-04.
static DAY_MONTH_YEAR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({MONTHS_ALT})\.?,?\s+(\d{{4}})\b"
    ))
    .expect("valid regex")
});

/// "July 2026", "Jul 2026", "Apr 28, 2027", "April 28 2027" → YYYY-MM.
static MONTH_YEAR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"\b({MONTHS_ALT})\.?\s+(?:(\d{{1,2}})(?:st|nd|rd|th)?\s*,?\s+)?(\d{{4}})\b"
    ))
    .expect("valid regex")
});

/// Kalshi-ticker-style "26JUL" / "27APR" (YYMMM) → 20YY-MM.
static YY_MMM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(\d{2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b")
        .expect("valid regex")
});

/// "4.25%", "0.10 %", "3 percent" → pct:<normalized>.
static PCT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:%|percent\b)").expect("valid regex")
});

/// "$4.50", "$150k", "$1,000" → usd:<normalized>.
static USD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(k|m|bn|b)?\b").expect("valid regex")
});

/// "150k", "1.5m", "2bn" → num:<normalized, expanded>.
static MAGNITUDE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(\d+(?:,\d{3})*(?:\.\d+)?)\s?(k|m|bn|b)\b").expect("valid regex")
});

/// Remaining bare numbers → num:<normalized>.
static BARE_NUM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b\d+(?:,\d{3})*(?:\.\d+)?\b").expect("valid regex"));

/// High-frequency glue words with no matching signal. Direction words
/// ("above", "below", "more", "than", "over", "under") are deliberately NOT
/// stopwords — they distinguish opposite markets.
const STOPWORDS: &[&str] = &[
    "will", "the", "be", "of", "in", "a", "an", "to", "at", "on", "by", "for", "is", "are", "it",
    "its", "this", "that", "do", "does", "what", "which", "as", "and", "or",
];

fn month_num(name: &str) -> Option<u32> {
    let idx = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ]
    .iter()
    .position(|m| name.starts_with(m))?;
    Some(idx as u32 + 1)
}

/// Parse a numeric capture (commas allowed), apply a multiplier, and render a
/// canonical form (trailing zeros stripped so `0.10%` == `0.1%`).
fn canonical_number(raw: &str, multiplier: i64) -> Option<String> {
    let cleaned = raw.replace(',', "");
    let d = Decimal::from_str(&cleaned).ok()?;
    Some((d * Decimal::from(multiplier)).normalize().to_string())
}

fn suffix_multiplier(suffix: Option<&str>) -> i64 {
    match suffix {
        Some("k") => 1_000,
        Some("m") => 1_000_000,
        Some("b") | Some("bn") => 1_000_000_000,
        _ => 1,
    }
}

/// Run `re` over `text`; feed each match through `f` collecting canonical
/// tokens into `out`, and return `text` with the matched spans blanked.
fn extract(
    re: &Regex,
    text: &str,
    out: &mut BTreeSet<String>,
    f: impl Fn(&Captures) -> Option<String>,
) -> String {
    let mut result = String::with_capacity(text.len());
    let mut last = 0;
    for caps in re.captures_iter(text) {
        let m = caps.get(0).expect("group 0 always present");
        if let Some(tok) = f(&caps) {
            out.insert(tok);
        }
        result.push_str(&text[last..m.start()]);
        result.push(' ');
        last = m.end();
    }
    result.push_str(&text[last..]);
    result
}

/// The normalized signal extracted from a market title (or description).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NormalizedTitle {
    /// Canonical month anchors, `"YYYY-MM"`.
    pub dates: BTreeSet<String>,
    /// Canonical numeric anchors: `"pct:4.25"`, `"usd:4.5"`, `"num:150000"`.
    pub thresholds: BTreeSet<String>,
    /// Lowercased content words, punctuation stripped, stopwords removed.
    pub tokens: BTreeSet<String>,
}

impl NormalizedTitle {
    /// The collision key for the rule pass, or `None` when the title carries
    /// too little signal to ever justify an `Exact` match.
    ///
    /// Requirements: at least one date or threshold anchor AND at least two
    /// content tokens. Titles like "Yes" or "Above 4.25%" alone never key.
    pub fn rule_key(&self) -> Option<String> {
        if self.tokens.len() < 2 || (self.dates.is_empty() && self.thresholds.is_empty()) {
            return None;
        }
        let join = |s: &BTreeSet<String>| s.iter().cloned().collect::<Vec<_>>().join(",");
        Some(format!(
            "d:{}|t:{}|w:{}",
            join(&self.dates),
            join(&self.thresholds),
            join(&self.tokens)
        ))
    }

    /// Union of all normalized signal, for set-similarity scoring.
    pub fn combined(&self) -> BTreeSet<String> {
        let mut set = self.tokens.clone();
        set.extend(self.dates.iter().cloned());
        set.extend(self.thresholds.iter().cloned());
        set
    }
}

/// Normalize free text: lowercase, canonicalize dates and thresholds, strip
/// punctuation, collapse whitespace, drop stopwords and single characters.
pub fn normalize(text: &str) -> NormalizedTitle {
    let lower = text.to_lowercase();
    let mut dates = BTreeSet::new();
    let mut thresholds = BTreeSet::new();

    // Dates first so their digits don't leak into bare-number extraction.
    let rest = extract(&DAY_MONTH_YEAR, &lower, &mut dates, |c| {
        let month = month_num(&c[2])?;
        Some(format!("{}-{month:02}", &c[3]))
    });
    let rest = extract(&MONTH_YEAR, &rest, &mut dates, |c| {
        let month = month_num(&c[1])?;
        Some(format!("{}-{month:02}", &c[3]))
    });
    let rest = extract(&YY_MMM, &rest, &mut dates, |c| {
        let month = month_num(&c[2])?;
        Some(format!("20{}-{month:02}", &c[1]))
    });

    // Units before bare numbers.
    let rest = extract(&PCT, &rest, &mut thresholds, |c| {
        Some(format!("pct:{}", canonical_number(&c[1], 1)?))
    });
    let rest = extract(&USD, &rest, &mut thresholds, |c| {
        let mult = suffix_multiplier(c.get(2).map(|m| m.as_str()));
        Some(format!("usd:{}", canonical_number(&c[1], mult)?))
    });
    let rest = extract(&MAGNITUDE, &rest, &mut thresholds, |c| {
        let mult = suffix_multiplier(c.get(2).map(|m| m.as_str()));
        Some(format!("num:{}", canonical_number(&c[1], mult)?))
    });
    let rest = extract(&BARE_NUM, &rest, &mut thresholds, |c| {
        Some(format!("num:{}", canonical_number(&c[0], 1)?))
    });

    let tokens = rest
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .filter(|w| w.len() >= 2 && !STOPWORDS.contains(w))
        .map(str::to_owned)
        .collect();

    NormalizedTitle {
        dates,
        thresholds,
        tokens,
    }
}

fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Sorted-legs identity key: the registry dedup key for a match.
pub fn legs_key(legs: &[InstrumentId]) -> String {
    let mut ids: Vec<&str> = legs.iter().map(|l| l.0.as_str()).collect();
    ids.sort_unstable();
    ids.join("|")
}

/// Deterministic, human-skimmable slug for a match: shared normalized tokens
/// plus a hash of the (sorted) legs for uniqueness.
pub fn make_match_id(instruments: &[&Instrument]) -> String {
    let mut shared: Option<BTreeSet<String>> = None;
    for inst in instruments {
        let combined = normalize(&inst.title).combined();
        shared = Some(match shared {
            None => combined,
            Some(prev) => prev.intersection(&combined).cloned().collect(),
        });
    }
    let mut parts: Vec<String> = shared
        .unwrap_or_default()
        .into_iter()
        .take(4)
        .map(|t| {
            t.chars()
                .map(|c| if c.is_alphanumeric() { c } else { '-' })
                .collect::<String>()
        })
        .collect();
    let legs: Vec<InstrumentId> = instruments.iter().map(|i| i.id.clone()).collect();
    parts.push(format!("{:08x}", fnv1a(&legs_key(&legs)) as u32));
    parts.join("-")
}

/// Maximum close-time disagreement (hours) between legs of an `Exact` match.
fn close_times_agree(instruments: &[&Instrument], guard_hours: i64) -> bool {
    let times: Vec<_> = instruments.iter().filter_map(|i| i.close_time).collect();
    for (i, a) in times.iter().enumerate() {
        for b in &times[i + 1..] {
            if (*a - *b).num_seconds().abs() > guard_hours * 3600 {
                return false;
            }
        }
    }
    true
}

/// The rule pass: normalized-key collisions between instruments of different
/// venues become `Exact`/`Rule` matches.
///
/// Conservatism rules (zero false `Exact` beats recall):
/// - the key requires a date or numeric anchor plus ≥2 content tokens;
/// - a key shared by more than one instrument of the same venue is ambiguous
///   and emits nothing;
/// - both legs' close times (when present) must agree within `guard_hours`.
pub fn rule_pass(instruments: &[&Instrument], guard_hours: i64) -> Vec<MatchedMarket> {
    let mut groups: BTreeMap<String, Vec<&Instrument>> = BTreeMap::new();
    for inst in instruments {
        if let Some(key) = normalize(&inst.title).rule_key() {
            groups.entry(key).or_default().push(inst);
        }
    }

    let mut out = Vec::new();
    for group in groups.values() {
        let venues: HashSet<Venue> = group.iter().map(|i| i.venue).collect();
        if venues.len() < 2 {
            continue; // no cross-venue collision
        }
        if group.len() != venues.len() {
            tracing::debug!(
                titles = ?group.iter().map(|i| &i.title).collect::<Vec<_>>(),
                "ambiguous rule-key collision (multiple instruments per venue); skipping"
            );
            continue;
        }
        if !close_times_agree(group, guard_hours) {
            tracing::debug!(
                titles = ?group.iter().map(|i| &i.title).collect::<Vec<_>>(),
                "rule-key collision but close times disagree; skipping"
            );
            continue;
        }
        let mut legs: Vec<InstrumentId> = group.iter().map(|i| i.id.clone()).collect();
        legs.sort_by(|a, b| a.0.cmp(&b.0));
        out.push(MatchedMarket {
            match_id: make_match_id(group),
            legs,
            confidence: MatchConfidence::Exact,
            method: MatchMethod::Rule,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

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

    #[test]
    fn dates_canonicalize_across_formats() {
        for (input, expected) in [
            ("in July 2026", "2026-07"),
            ("in Jul 2026", "2026-07"),
            ("in jul. 2026", "2026-07"),
            ("following the Fed's Apr 28, 2027 meeting", "2027-04"),
            ("on April 28 2027", "2027-04"),
            ("by 28 April 2027", "2027-04"),
            ("ticker mentions 26JUL", "2026-07"),
            ("ticker mentions 27APR", "2027-04"),
            ("by December 31, 2026", "2026-12"),
        ] {
            let n = normalize(input);
            assert_eq!(
                n.dates.iter().collect::<Vec<_>>(),
                vec![expected],
                "input: {input}"
            );
        }
    }

    #[test]
    fn date_digits_do_not_leak_into_numbers() {
        let n = normalize("following the Fed's Apr 28, 2027 meeting");
        assert!(n.thresholds.is_empty(), "got {:?}", n.thresholds);
    }

    #[test]
    fn thresholds_canonicalize() {
        assert!(normalize("above 4.25%").thresholds.contains("pct:4.25"));
        // trailing zeros stripped: 0.10% == 0.1%
        assert_eq!(
            normalize("rise more than 0.10%").thresholds,
            normalize("rise more than 0.1%").thresholds
        );
        assert!(normalize("payrolls above 150k")
            .thresholds
            .contains("num:150000"));
        assert!(normalize("above 1.5m barrels")
            .thresholds
            .contains("num:1500000"));
        assert!(normalize("gas above $4.50").thresholds.contains("usd:4.5"));
        assert_eq!(
            normalize("gas above $4.50").thresholds,
            normalize("gas above $4.5").thresholds
        );
        assert!(normalize("above 1,000 points")
            .thresholds
            .contains("num:1000"));
        assert!(normalize("3 percent mortgage").thresholds.contains("pct:3"));
    }

    #[test]
    fn tokens_lowercase_strip_punct_drop_stopwords() {
        let n = normalize("Will the S&P-500 CLOSE higher?!");
        assert!(n.tokens.contains("close"));
        assert!(n.tokens.contains("higher"));
        assert!(!n.tokens.contains("will"));
        assert!(!n.tokens.contains("the"));
        // "S&P-500" → "s"(dropped, len 1) + "p"(dropped) + 500 threshold
        assert!(n.thresholds.contains("num:500"));
    }

    #[test]
    fn weak_titles_produce_no_rule_key() {
        assert_eq!(normalize("Yes").rule_key(), None);
        assert_eq!(normalize("Above 4.25%").rule_key(), None); // one content token
        assert_eq!(normalize("Will Biden win the election?").rule_key(), None); // no anchor
    }

    #[test]
    fn cpi_pair_matches_exact_after_normalization() {
        // Real Kalshi phrasing (docs/research/kalshi-api.md style) vs the same
        // market with abbreviated month + different punctuation/case.
        let k = inst(
            Venue::Kalshi,
            "KXCPI-26JUL-T0.0",
            "Will CPI rise more than 0.0% in July 2026?",
            Some("2026-08-12T12:00:00Z"),
        );
        let p = inst(
            Venue::Polymarket,
            "0xcpi26jul",
            "Will CPI rise more than 0.0% in Jul 2026",
            Some("2026-08-12T00:00:00Z"),
        );
        let matches = rule_pass(&[&k, &p], 72);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].confidence, MatchConfidence::Exact);
        assert_eq!(matches[0].method, MatchMethod::Rule);
        assert_eq!(matches[0].legs.len(), 2);
    }

    #[test]
    fn different_threshold_never_matches_exact() {
        // Same event, different strike: must NOT collide.
        let k = inst(
            Venue::Kalshi,
            "KXFED-27APR-T4.25",
            "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
            Some("2027-04-28T18:00:00Z"),
        );
        let p = inst(
            Venue::Polymarket,
            "0xfed450",
            "Will the upper bound of the federal funds rate be above 4.50% following the Fed's Apr 28, 2027 meeting?",
            Some("2027-04-28T18:00:00Z"),
        );
        assert!(rule_pass(&[&k, &p], 72).is_empty());
    }

    #[test]
    fn same_venue_collision_is_not_a_match() {
        let a = inst(
            Venue::Kalshi,
            "A",
            "Will CPI rise more than 0.0% in July 2026?",
            None,
        );
        let b = inst(
            Venue::Kalshi,
            "B",
            "Will CPI rise more than 0.0% in July 2026?",
            None,
        );
        assert!(rule_pass(&[&a, &b], 72).is_empty());
    }

    #[test]
    fn ambiguous_collision_is_skipped() {
        // Two Kalshi instruments + one Polymarket sharing a key: ambiguous.
        let a = inst(
            Venue::Kalshi,
            "A",
            "Will CPI rise more than 0.0% in July 2026?",
            None,
        );
        let b = inst(
            Venue::Kalshi,
            "B",
            "Will CPI rise more than 0.0% in July 2026?",
            None,
        );
        let c = inst(
            Venue::Polymarket,
            "C",
            "Will CPI rise more than 0.0% in July 2026?",
            None,
        );
        assert!(rule_pass(&[&a, &b, &c], 72).is_empty());
    }

    #[test]
    fn disagreeing_close_times_block_exact() {
        let k = inst(
            Venue::Kalshi,
            "K",
            "Will CPI rise more than 0.0% in July 2026?",
            Some("2026-08-12T12:00:00Z"),
        );
        let p = inst(
            Venue::Polymarket,
            "P",
            "Will CPI rise more than 0.0% in July 2026?",
            Some("2026-09-30T12:00:00Z"), // > 72h apart
        );
        assert!(rule_pass(&[&k, &p], 72).is_empty());
    }

    #[test]
    fn match_id_is_deterministic_and_sluggy() {
        let k = inst(
            Venue::Kalshi,
            "K1",
            "Will CPI rise more than 0.0% in July 2026?",
            None,
        );
        let p = inst(
            Venue::Polymarket,
            "P1",
            "Will CPI rise more than 0.0% in Jul 2026",
            None,
        );
        let id1 = make_match_id(&[&k, &p]);
        let id2 = make_match_id(&[&k, &p]);
        assert_eq!(id1, id2);
        assert!(id1.contains("cpi"), "got {id1}");
        assert!(id1.is_ascii());
    }
}
