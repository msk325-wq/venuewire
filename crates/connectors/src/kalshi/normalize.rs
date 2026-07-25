//! Kalshi payload normalization: REST market objects and WS `ticker` messages
//! into canonical [`Tick`]s / [`Instrument`]s.
//!
//! This module is the *single* normalization path: the live connector and the
//! replay connector both feed raw frames through [`normalize_frame`], so a
//! recorded fixture replays byte-for-byte through the same code as live data.
//!
//! Price handling (docs/research/kalshi-api.md §3): Kalshi has migrated to
//! decimal **dollar strings** (`"0.2700"`, fields suffixed `_dollars`), already
//! probabilities in `[0, 1]`. Legacy integer-cent fields (`yes_bid: 27`) are
//! tolerated as a fallback via [`vw_core::types::kalshi_cents_to_prob`].

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;
use vw_core::types::kalshi_cents_to_prob;
use vw_core::{Instrument, InstrumentId, RawFrame, Tick, Venue};

use crate::ConnectorEvent;

/// Parse a Kalshi dollar-string price (`"0.2700"`) into a probability.
fn dollars(obj: &Value, key: &str) -> Option<Decimal> {
    let s = obj.get(key)?.as_str()?;
    Decimal::from_str(s).ok()
}

/// Parse a legacy integer-cent price (`27`) into a probability.
fn cents(obj: &Value, key: &str) -> Option<Decimal> {
    let n = obj.get(key)?;
    // Legacy cents were JSON integers; accept i64/u64 but not strings (those
    // are the new dollar fields, handled above).
    n.as_i64().map(|c| kalshi_cents_to_prob(Decimal::from(c)))
}

/// Preferred dollar-string field with legacy integer-cent fallback.
fn price(obj: &Value, dollar_key: &str, cent_key: &str) -> Option<Decimal> {
    dollars(obj, dollar_key).or_else(|| cents(obj, cent_key))
}

fn rfc3339(obj: &Value, key: &str) -> Option<DateTime<Utc>> {
    let s = obj.get(key)?.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// WS `ts_ms` (canonical per research notes; `ts`/`time` are deprecated).
fn ts_ms(obj: &Value) -> Option<DateTime<Utc>> {
    let ms = obj.get("ts_ms")?.as_i64()?;
    Utc.timestamp_millis_opt(ms).single()
}

/// Normalize a REST `GET /markets` market object into metadata + a snapshot tick.
///
/// Returns `None` when the object has no `ticker` (not a market payload).
pub fn market_snapshot(
    market: &Value,
    recv_ts: DateTime<Utc>,
    seq: &mut u64,
) -> Option<(Instrument, Tick)> {
    let ticker = market.get("ticker")?.as_str()?;
    let id = InstrumentId::new(Venue::Kalshi, ticker);
    let instrument = Instrument {
        id: id.clone(),
        venue: Venue::Kalshi,
        title: market
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or(ticker)
            .to_string(),
        description: market
            .get("rules_primary")
            .and_then(Value::as_str)
            .map(str::to_string),
        close_time: rfc3339(market, "close_time"),
        // The market payload carries no category; series metadata does (M3).
        category: None,
        raw: market.clone(),
    };
    *seq += 1;
    let tick = Tick {
        instrument: id,
        venue: Venue::Kalshi,
        yes_bid: price(market, "yes_bid_dollars", "yes_bid"),
        yes_ask: price(market, "yes_ask_dollars", "yes_ask"),
        last_price: price(market, "last_price_dollars", "last_price"),
        venue_ts: rfc3339(market, "updated_time"),
        recv_ts,
        seq: *seq,
    };
    Some((instrument, tick))
}

/// Normalize the `msg` body of a WS `ticker` message into a tick.
pub fn ticker_msg(msg: &Value, recv_ts: DateTime<Utc>, seq: &mut u64) -> Option<Tick> {
    let ticker = msg.get("market_ticker")?.as_str()?;
    *seq += 1;
    Some(Tick {
        instrument: InstrumentId::new(Venue::Kalshi, ticker),
        venue: Venue::Kalshi,
        yes_bid: price(msg, "yes_bid_dollars", "yes_bid"),
        yes_ask: price(msg, "yes_ask_dollars", "yes_ask"),
        last_price: price(msg, "price_dollars", "price"),
        venue_ts: ts_ms(msg),
        recv_ts,
        seq: *seq,
    })
}

/// Normalize one raw Kalshi frame (live or replayed) into connector events.
///
/// Dispatches on shape:
/// - WS envelope with `"type": "ticker"` → one [`ConnectorEvent::Tick`].
/// - REST market object (has `"ticker"`) → [`ConnectorEvent::Instrument`] +
///   snapshot [`ConnectorEvent::Tick`].
/// - Anything else (subscription confirmations, errors, unknown channels) →
///   empty; the connector logs those out-of-band.
pub fn normalize_frame(frame: &RawFrame, seq: &mut u64) -> Vec<ConnectorEvent> {
    let raw = &frame.raw_frame;
    if let Some(msg_type) = raw.get("type").and_then(Value::as_str) {
        if msg_type == "ticker" {
            if let Some(msg) = raw.get("msg") {
                if let Some(tick) = ticker_msg(msg, frame.recv_ts, seq) {
                    return vec![ConnectorEvent::Tick(tick)];
                }
            }
        }
        return Vec::new();
    }
    if raw.get("ticker").is_some() {
        if let Some((instrument, tick)) = market_snapshot(raw, frame.recv_ts, seq) {
            return vec![
                ConnectorEvent::Instrument(instrument),
                ConnectorEvent::Tick(tick),
            ];
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn frame(raw: Value) -> RawFrame {
        RawFrame {
            recv_ts: Utc::now(),
            venue: Venue::Kalshi,
            raw_frame: raw,
        }
    }

    /// REAL payload: live capture 2026-07-18, docs/research/kalshi-api.md §1
    /// (`GET /markets?series_ticker=KXFED&status=open&limit=2`), abridged to
    /// the fields normalization reads plus a few extras.
    fn live_rest_market() -> Value {
        serde_json::json!({
            "can_close_early": true,
            "close_time": "2027-04-28T17:55:00Z",
            "created_time": "2025-10-06T19:22:55.893694Z",
            "event_ticker": "KXFED-27APR",
            "floor_strike": 4.25,
            "last_price_dollars": "0.2800",
            "market_type": "binary",
            "no_ask_dollars": "0.7300",
            "no_bid_dollars": "0.7200",
            "no_sub_title": "Above 4.25%",
            "notional_value_dollars": "1.0000",
            "open_interest_fp": "2020.47",
            "price_level_structure": "linear_cent",
            "rules_primary": "If the upper bound of the target federal funds rate ... resolves to Yes.",
            "status": "active",
            "strike_type": "greater",
            "subtitle": "4.25%",
            "ticker": "KXFED-27APR-T4.25",
            "title": "Will the upper bound of the federal funds rate be above 4.25% following the Fed's Apr 28, 2027 meeting?",
            "updated_time": "2026-04-09T14:07:28.591885Z",
            "volume_fp": "10155.43",
            "yes_ask_dollars": "0.2800",
            "yes_ask_size_fp": "20.00",
            "yes_bid_dollars": "0.2700",
            "yes_bid_size_fp": "31.54",
            "yes_sub_title": "Above 4.25%"
        })
    }

    /// DOC-SOURCED payload: verbatim `ticker` example from Kalshi's
    /// asyncapi.yaml (docs/research/kalshi-api.md §2, not live-captured —
    /// live WSS requires auth).
    fn doc_ws_ticker() -> Value {
        serde_json::json!({
            "type": "ticker",
            "sid": 11,
            "msg": {
                "market_ticker": "FED-23DEC-T3.00",
                "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
                "price_dollars": "0.480",
                "yes_bid_dollars": "0.450",
                "yes_ask_dollars": "0.530",
                "volume_fp": "33896.00",
                "open_interest_fp": "20422.00",
                "dollar_volume": 16948,
                "dollar_open_interest": 10211,
                "yes_bid_size_fp": "300.00",
                "yes_ask_size_fp": "150.00",
                "last_trade_size_fp": "25.00",
                "ts": 1669149841i64,
                "ts_ms": 1669149841000i64,
                "time": "2022-11-22T20:44:01Z"
            }
        })
    }

    #[test]
    fn rest_market_normalizes_dollars_and_metadata() {
        let mut seq = 0;
        let (inst, tick) = market_snapshot(&live_rest_market(), Utc::now(), &mut seq).unwrap();
        assert_eq!(inst.id.0, "kalshi:KXFED-27APR-T4.25");
        assert_eq!(inst.venue, Venue::Kalshi);
        assert!(inst.title.starts_with("Will the upper bound"));
        assert!(inst
            .description
            .as_deref()
            .unwrap()
            .contains("target federal funds rate"));
        assert_eq!(
            inst.close_time.unwrap(),
            Utc.with_ymd_and_hms(2027, 4, 28, 17, 55, 0).unwrap()
        );
        // raw payload preserved verbatim
        assert_eq!(inst.raw["yes_bid_size_fp"], "31.54");

        assert_eq!(tick.yes_bid, Some(dec!(0.2700)));
        assert_eq!(tick.yes_ask, Some(dec!(0.2800)));
        assert_eq!(tick.last_price, Some(dec!(0.2800)));
        assert_eq!(tick.seq, 1);
        assert_eq!(
            tick.venue_ts.unwrap().to_rfc3339(),
            "2026-04-09T14:07:28.591885+00:00"
        );
    }

    #[test]
    fn ws_ticker_normalizes_dollars_and_ts_ms() {
        let mut seq = 41;
        let events = normalize_frame(&frame(doc_ws_ticker()), &mut seq);
        assert_eq!(events.len(), 1);
        let ConnectorEvent::Tick(tick) = &events[0] else {
            panic!("expected tick");
        };
        assert_eq!(tick.instrument.0, "kalshi:FED-23DEC-T3.00");
        assert_eq!(tick.yes_bid, Some(dec!(0.450)));
        assert_eq!(tick.yes_ask, Some(dec!(0.530)));
        assert_eq!(tick.last_price, Some(dec!(0.480)));
        assert_eq!(tick.seq, 42);
        // ts_ms is canonical, not the deprecated `ts`/`time`
        assert_eq!(tick.venue_ts.unwrap().timestamp_millis(), 1669149841000);
    }

    /// SYNTHETIC payload: legacy integer-cent shape (pre-migration API), kept
    /// for tolerance per docs/research/kalshi-api.md §3 — no live example
    /// exists anymore.
    #[test]
    fn legacy_integer_cents_fall_back_via_cents_to_prob() {
        let raw = serde_json::json!({
            "ticker": "FED-23DEC-T3.00",
            "title": "Fed above 3.00%",
            "yes_bid": 27,
            "yes_ask": 29,
            "last_price": 28
        });
        let mut seq = 0;
        let (_, tick) = market_snapshot(&raw, Utc::now(), &mut seq).unwrap();
        assert_eq!(tick.yes_bid, Some(dec!(0.27)));
        assert_eq!(tick.yes_ask, Some(dec!(0.29)));
        assert_eq!(tick.last_price, Some(dec!(0.28)));

        let ws = serde_json::json!({
            "type": "ticker",
            "sid": 1,
            "msg": { "market_ticker": "FED-23DEC-T3.00", "price": 48, "yes_bid": 45, "yes_ask": 53 }
        });
        let events = normalize_frame(&frame(ws), &mut seq);
        let ConnectorEvent::Tick(tick) = &events[0] else {
            panic!("expected tick");
        };
        assert_eq!(tick.yes_bid, Some(dec!(0.45)));
        assert_eq!(tick.last_price, Some(dec!(0.48)));
    }

    #[test]
    fn rest_frame_yields_instrument_then_tick() {
        let mut seq = 0;
        let events = normalize_frame(&frame(live_rest_market()), &mut seq);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], ConnectorEvent::Instrument(_)));
        assert!(matches!(events[1], ConnectorEvent::Tick(_)));
    }

    #[test]
    fn non_data_frames_are_ignored() {
        let mut seq = 7;
        // subscription confirmation (doc-sourced shape)
        let sub = serde_json::json!({ "id": 1, "type": "subscribed", "msg": { "channel": "ticker", "sid": 1 } });
        assert!(normalize_frame(&frame(sub), &mut seq).is_empty());
        // error frame
        let err = serde_json::json!({ "id": 2, "type": "error", "msg": { "code": 9, "msg": "auth required" } });
        assert!(normalize_frame(&frame(err), &mut seq).is_empty());
        // garbage
        assert!(
            normalize_frame(&frame(serde_json::json!({"hello": "world"})), &mut seq).is_empty()
        );
        assert_eq!(seq, 7, "ignored frames must not consume sequence numbers");
    }

    #[test]
    fn missing_prices_are_none_not_zero() {
        let raw = serde_json::json!({ "ticker": "KXCPI-X", "title": "t" });
        let mut seq = 0;
        let (_, tick) = market_snapshot(&raw, Utc::now(), &mut seq).unwrap();
        assert_eq!(tick.yes_bid, None);
        assert_eq!(tick.yes_ask, None);
        assert_eq!(tick.last_price, None);
    }
}
