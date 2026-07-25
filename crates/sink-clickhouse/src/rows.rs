//! JSONEachRow encoding matching the spec §7.3 schema.
//!
//! - `Decimal(9,6)` columns are sent as **strings** (`"0.42"`), which
//!   ClickHouse parses into decimals without any float round-trip.
//! - `DateTime64(3, 'UTC')` columns are sent as `YYYY-MM-DD hh:mm:ss.mmm`
//!   strings (millisecond precision, UTC), ClickHouse's canonical text form.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use vw_core::{DivergenceEvent, Tick};

#[derive(Serialize)]
struct TickRow<'a> {
    instrument: &'a str,
    venue: &'a str,
    yes_bid: Option<String>,
    yes_ask: Option<String>,
    last_price: Option<String>,
    venue_ts: Option<String>,
    recv_ts: String,
    seq: u64,
}

#[derive(Serialize)]
struct DivergenceRow<'a> {
    match_id: &'a str,
    spread: String,
    /// The legs `[[instrument, mid], ...]` as an embedded JSON string.
    legs: String,
    detected_at: String,
}

/// Render a decimal for a `Decimal(9,6)` column: at most 6 fractional digits,
/// exact (no float round-trip).
fn dec6(d: Decimal) -> String {
    d.round_dp(6).to_string()
}

/// Render a timestamp for a `DateTime64(3, 'UTC')` column.
fn ts_ms(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

/// One JSONEachRow line (no trailing newline) for the `ticks` table.
pub fn tick_row(t: &Tick) -> String {
    let row = TickRow {
        instrument: &t.instrument.0,
        venue: t.venue.as_str(),
        yes_bid: t.yes_bid.map(dec6),
        yes_ask: t.yes_ask.map(dec6),
        last_price: t.last_price.map(dec6),
        venue_ts: t.venue_ts.map(ts_ms),
        recv_ts: ts_ms(t.recv_ts),
        seq: t.seq,
    };
    serde_json::to_string(&row).expect("tick row serialization cannot fail")
}

/// One JSONEachRow line (no trailing newline) for the `divergences` table.
pub fn divergence_row(e: &DivergenceEvent) -> String {
    let legs: Vec<(&str, String)> = e
        .legs
        .iter()
        .map(|(id, mid)| (id.0.as_str(), dec6(*mid)))
        .collect();
    let row = DivergenceRow {
        match_id: &e.match_id,
        spread: dec6(e.spread),
        legs: serde_json::to_string(&legs).expect("legs serialization cannot fail"),
        detected_at: ts_ms(e.detected_at),
    };
    serde_json::to_string(&row).expect("divergence row serialization cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;
    use vw_core::{InstrumentId, Venue};

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 18, 12, 30, 45)
            .unwrap()
            .checked_add_signed(chrono::Duration::milliseconds(123))
            .unwrap()
    }

    #[test]
    fn tick_row_encodes_decimals_as_strings_and_ms_timestamps() {
        let t = Tick {
            instrument: InstrumentId("kalshi:FED-26JUL".into()),
            venue: Venue::Kalshi,
            yes_bid: Some(dec!(0.42)),
            yes_ask: Some(dec!(0.4400005)), // 7 dp: must round to Decimal(9,6) scale
            last_price: None,
            venue_ts: None,
            recv_ts: ts(),
            seq: 7,
        };
        let line = tick_row(&t);
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["instrument"], "kalshi:FED-26JUL");
        assert_eq!(v["venue"], "kalshi");
        assert_eq!(v["yes_bid"], "0.42");
        assert_eq!(v["yes_ask"], "0.440000"); // banker's rounding at 6 dp
        assert_eq!(v["last_price"], serde_json::Value::Null);
        assert_eq!(v["venue_ts"], serde_json::Value::Null);
        assert_eq!(v["recv_ts"], "2026-07-18 12:30:45.123");
        assert_eq!(v["seq"], 7);
        assert!(!line.contains('\n'), "one line per row");
    }

    #[test]
    fn divergence_row_embeds_legs_as_json_string() {
        let e = DivergenceEvent {
            match_id: "fed-26jul".into(),
            spread: dec!(0.05),
            legs: vec![
                (InstrumentId("kalshi:FED-26JUL".into()), dec!(0.40)),
                (InstrumentId("polymarket:0xabc".into()), dec!(0.45)),
            ],
            detected_at: ts(),
        };
        let line = divergence_row(&e);
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["match_id"], "fed-26jul");
        assert_eq!(v["spread"], "0.05");
        assert_eq!(v["detected_at"], "2026-07-18 12:30:45.123");
        // legs is a *string* column containing JSON
        let legs: Vec<(String, String)> =
            serde_json::from_str(v["legs"].as_str().unwrap()).unwrap();
        assert_eq!(
            legs[0],
            ("kalshi:FED-26JUL".to_string(), "0.40".to_string())
        );
        assert_eq!(
            legs[1],
            ("polymarket:0xabc".to_string(), "0.45".to_string())
        );
    }
}
