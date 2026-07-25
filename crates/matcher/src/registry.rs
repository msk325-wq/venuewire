//! The human-editable YAML match registry (`config/matches.yaml`).
//!
//! The registry is the source of truth for cross-venue matches. The engine
//! reloads it at the start of every pass (picking up human edits), merges new
//! findings, and writes it back atomically (temp file + rename). Semantics:
//!
//! - `method: manual` entries override everything the engine computes;
//! - `status: rejected` entries are never re-emitted;
//! - engine-written entries get `status: active` for `Exact`/`High`
//!   confidence and `status: review` for `Review`.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use vw_core::{InstrumentId, MatchConfidence, MatchMethod, MatchedMarket};

/// Lifecycle of a registry entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchStatus {
    /// Trusted: included in divergence detection.
    Active,
    /// Awaiting human confirmation; excluded from divergence detection.
    Review,
    /// Human said no: the engine must never re-emit this leg set.
    Rejected,
}

/// One persisted match.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub match_id: String,
    pub legs: Vec<InstrumentId>,
    pub confidence: MatchConfidence,
    pub method: MatchMethod,
    pub status: MatchStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl RegistryEntry {
    /// Sorted-legs identity key (registry dedup key).
    pub fn legs_key(&self) -> String {
        crate::rules::legs_key(&self.legs)
    }
}

const HEADER: &str = "\
# venuewire cross-venue match registry (M3). Human-editable.
#
# - `method: manual` entries override everything the engine computes.
# - `status: rejected` entries are never re-emitted by the engine.
# - `status: review` entries await promotion and are excluded from
#   divergence detection.
# See config/matches.example.yaml for an annotated example.
";

/// The full registry file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Registry {
    #[serde(default)]
    pub entries: Vec<RegistryEntry>,
}

impl Registry {
    /// Load from `path`. A missing or empty file is an empty registry;
    /// malformed YAML is an error (never silently discard human edits).
    pub fn load(path: &Path) -> anyhow::Result<Registry> {
        if !path.exists() {
            return Ok(Registry::default());
        }
        let text = fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading registry {}: {e}", path.display()))?;
        if text.trim().is_empty() {
            return Ok(Registry::default());
        }
        serde_yaml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing registry {}: {e}", path.display()))
    }

    /// Atomic write: serialize to a sibling temp file, then rename over the
    /// destination so readers (and a crash mid-write) never see a torn file.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let yaml = serde_yaml::to_string(self)?;
        let tmp = path.with_extension(format!("yaml.tmp.{}", std::process::id()));
        fs::write(&tmp, format!("{HEADER}{yaml}"))
            .map_err(|e| anyhow::anyhow!("writing {}: {e}", tmp.display()))?;
        fs::rename(&tmp, path)
            .map_err(|e| anyhow::anyhow!("renaming {} -> {}: {e}", tmp.display(), path.display()))
    }

    /// Index of the entry with the given sorted-legs key, if any.
    pub fn find_by_legs(&self, legs_key: &str) -> Option<usize> {
        self.entries.iter().position(|e| e.legs_key() == legs_key)
    }

    /// The matches downstream consumers (divergence detection) should trust.
    pub fn active_matches(&self) -> Vec<MatchedMarket> {
        self.entries
            .iter()
            .filter(|e| e.status == MatchStatus::Active)
            .map(|e| MatchedMarket {
                match_id: e.match_id.clone(),
                legs: e.legs.clone(),
                confidence: e.confidence,
                method: e.method,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vw_core::Venue;

    fn sample() -> Registry {
        Registry {
            entries: vec![
                RegistryEntry {
                    match_id: "cpi-2026-07-abc123".into(),
                    legs: vec![
                        InstrumentId::new(Venue::Kalshi, "KXCPI-26JUL-T0.0"),
                        InstrumentId::new(Venue::Polymarket, "0xcpi26jul"),
                    ],
                    confidence: MatchConfidence::Exact,
                    method: MatchMethod::Rule,
                    status: MatchStatus::Active,
                    note: None,
                },
                RegistryEntry {
                    match_id: "fed-2027-04-def456".into(),
                    legs: vec![
                        InstrumentId::new(Venue::Kalshi, "KXFED-27APR-T4.25"),
                        InstrumentId::new(Venue::Polymarket, "0xfed425"),
                    ],
                    confidence: MatchConfidence::Review,
                    method: MatchMethod::Embedding,
                    status: MatchStatus::Review,
                    note: Some("lexical score 0.62".into()),
                },
                RegistryEntry {
                    match_id: "bogus-ghi789".into(),
                    legs: vec![
                        InstrumentId::new(Venue::Kalshi, "KXWRONG"),
                        InstrumentId::new(Venue::Polymarket, "0xwrong"),
                    ],
                    confidence: MatchConfidence::High,
                    method: MatchMethod::Manual,
                    status: MatchStatus::Rejected,
                    note: Some("same topic, different resolution source".into()),
                },
            ],
        }
    }

    #[test]
    fn round_trip_through_yaml_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("matches.yaml");
        let reg = sample();
        reg.save(&path).unwrap();
        let loaded = Registry::load(&path).unwrap();
        assert_eq!(reg, loaded);
        // header comment present, no temp file left behind
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.starts_with("# venuewire"));
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn missing_and_empty_files_load_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.yaml");
        assert_eq!(Registry::load(&missing).unwrap(), Registry::default());
        let empty = dir.path().join("empty.yaml");
        std::fs::write(&empty, "\n# just a comment\n").unwrap();
        assert_eq!(Registry::load(&empty).unwrap(), Registry::default());
    }

    #[test]
    fn malformed_yaml_is_an_error_not_a_wipe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.yaml");
        std::fs::write(&path, "entries: [ {match_id: broken").unwrap();
        assert!(Registry::load(&path).is_err());
    }

    #[test]
    fn save_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/deeper/matches.yaml");
        sample().save(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn legs_key_is_order_insensitive() {
        let mut e = sample().entries[0].clone();
        let k1 = e.legs_key();
        e.legs.reverse();
        assert_eq!(k1, e.legs_key());
    }

    #[test]
    fn active_matches_filters_by_status() {
        let reg = sample();
        let active = reg.active_matches();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].match_id, "cpi-2026-07-abc123");
    }

    #[test]
    fn human_edited_yaml_parses() {
        // The shape a human would write by hand (matches the example file).
        let text = r#"
entries:
  - match_id: fed-rate-2027-04
    legs: ["kalshi:KXFED-27APR-T4.25", "polymarket:0xfed425"]
    confidence: high
    method: manual
    status: active
    note: verified by hand 2026-07-18
"#;
        let reg: Registry = serde_yaml::from_str(text).unwrap();
        assert_eq!(reg.entries.len(), 1);
        assert_eq!(reg.entries[0].method, MatchMethod::Manual);
        assert_eq!(reg.entries[0].status, MatchStatus::Active);
        assert_eq!(reg.entries[0].legs[0].0, "kalshi:KXFED-27APR-T4.25");
    }
}
