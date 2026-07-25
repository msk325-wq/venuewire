//! The canonical data model shared by every venuewire component.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Kalshi,
    Polymarket,
}

impl Venue {
    pub fn as_str(&self) -> &'static str {
        match self {
            Venue::Kalshi => "kalshi",
            Venue::Polymarket => "polymarket",
        }
    }
}

impl fmt::Display for Venue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Stable internal id: `{venue}:{venue_native_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InstrumentId(pub String);

impl InstrumentId {
    pub fn new(venue: Venue, native_id: &str) -> Self {
        InstrumentId(format!("{venue}:{native_id}"))
    }
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A single normalized market update.
///
/// Prices are probabilities for the YES side in `[0, 1]`, always `Decimal` —
/// never a float.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub instrument: InstrumentId,
    pub venue: Venue,
    pub yes_bid: Option<Decimal>,
    pub yes_ask: Option<Decimal>,
    pub last_price: Option<Decimal>,
    /// Venue-reported event time, if the venue provides one.
    pub venue_ts: Option<DateTime<Utc>>,
    /// Captured immediately after the frame is read off the socket, before parsing.
    pub recv_ts: DateTime<Utc>,
    /// Connector-local monotonic sequence.
    pub seq: u64,
}

impl Tick {
    /// Mid of the YES bid/ask, falling back to whichever side exists.
    pub fn yes_mid(&self) -> Option<Decimal> {
        match (self.yes_bid, self.yes_ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::TWO),
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        }
    }
}

/// Static/slow-changing metadata about a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instrument {
    pub id: InstrumentId,
    pub venue: Venue,
    pub title: String,
    pub description: Option<String>,
    pub close_time: Option<DateTime<Utc>>,
    pub category: Option<String>,
    /// Original venue payload, preserved verbatim.
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchConfidence {
    Exact,
    High,
    Review,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchMethod {
    Rule,
    Embedding,
    Manual,
}

/// A resolved cross-venue equivalence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchedMarket {
    /// Stable slug.
    pub match_id: String,
    /// At least two legs.
    pub legs: Vec<InstrumentId>,
    pub confidence: MatchConfidence,
    pub method: MatchMethod,
}

/// Emitted when matched legs disagree beyond the configured threshold.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergenceEvent {
    pub match_id: String,
    /// `|p_a - p_b|` using mid prices.
    pub spread: Decimal,
    pub legs: Vec<(InstrumentId, Decimal)>,
    pub detected_at: DateTime<Utc>,
}

/// A raw venue frame captured pre-normalization, as recorded to NDJSON
/// fixtures (`{recv_ts, venue, raw_frame}`) and replayed by the replay
/// connector through the same normalization code as live.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawFrame {
    /// Captured immediately after the frame was read, before parsing.
    pub recv_ts: DateTime<Utc>,
    pub venue: Venue,
    /// The venue payload verbatim (WS message or REST-derived object).
    pub raw_frame: serde_json::Value,
}

/// Kalshi prices arrive in cents (0–100); normalize to a probability in `[0, 1]`.
pub fn kalshi_cents_to_prob(cents: Decimal) -> Decimal {
    cents / Decimal::ONE_HUNDRED
}

/// Derive the YES probability when only NO-side data is available.
pub fn yes_from_no(p_no: Decimal) -> Decimal {
    Decimal::ONE - p_no
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn instrument_id_format() {
        let id = InstrumentId::new(Venue::Kalshi, "FED-26JUL-T4.50");
        assert_eq!(id.0, "kalshi:FED-26JUL-T4.50");
        let id = InstrumentId::new(Venue::Polymarket, "0xabc123");
        assert_eq!(id.0, "polymarket:0xabc123");
    }

    #[test]
    fn kalshi_normalization_is_exact() {
        assert_eq!(kalshi_cents_to_prob(dec!(37)), dec!(0.37));
        assert_eq!(kalshi_cents_to_prob(dec!(0)), dec!(0));
        assert_eq!(kalshi_cents_to_prob(dec!(100)), dec!(1));
        // sub-cent ticks must not lose precision
        assert_eq!(kalshi_cents_to_prob(dec!(3.5)), dec!(0.035));
    }

    #[test]
    fn yes_from_no_is_exact() {
        assert_eq!(yes_from_no(dec!(0.42)), dec!(0.58));
        assert_eq!(yes_from_no(dec!(1)), dec!(0));
    }

    #[test]
    fn yes_mid_prefers_both_sides() {
        let tick = Tick {
            instrument: InstrumentId::new(Venue::Kalshi, "X"),
            venue: Venue::Kalshi,
            yes_bid: Some(dec!(0.40)),
            yes_ask: Some(dec!(0.44)),
            last_price: None,
            venue_ts: None,
            recv_ts: Utc::now(),
            seq: 1,
        };
        assert_eq!(tick.yes_mid(), Some(dec!(0.42)));

        let one_sided = Tick {
            yes_ask: None,
            ..tick.clone()
        };
        assert_eq!(one_sided.yes_mid(), Some(dec!(0.40)));

        let empty = Tick {
            yes_bid: None,
            yes_ask: None,
            ..tick
        };
        assert_eq!(empty.yes_mid(), None);
    }

    #[test]
    fn venue_serde_roundtrip() {
        let json = serde_json::to_string(&Venue::Polymarket).unwrap();
        assert_eq!(json, "\"polymarket\"");
        let back: Venue = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Venue::Polymarket);
    }
}
