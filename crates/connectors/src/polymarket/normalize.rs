//! Polymarket payload normalization: Gamma REST market objects and CLOB WSS
//! `book` / `price_change` / `last_trade_price` events into canonical
//! [`Tick`]s / [`Instrument`]s.
//!
//! Like the Kalshi module, this is the *single* normalization path — live
//! connector and replay both feed raw frames through
//! [`Normalizer::normalize_frame`] — but unlike Kalshi it is **stateful**:
//! WSS events are keyed by outcome *token id* and carry no outcome side, so
//! the normalizer learns each market's token → (condition id, yes-leg)
//! mapping from the Gamma discovery frames that precede WSS frames in every
//! stream (live and fixture alike). WSS frames for unknown tokens are skipped.
//!
//! Conventions (docs/research/polymarket-api.md, decisions.md D10):
//! - Instrument/tick identity is the market's `conditionId`
//!   (`polymarket:0x…`); token ids are transport-level subscribe keys only.
//! - The "yes leg" is the token whose outcome label is `"Yes"`
//!   (case-insensitive); for non-Yes/No markets (sports teams, candidates)
//!   it is outcome index 0, matching Gamma's `clobTokenIds`/`outcomes` order.
//! - NO-leg frames are complemented: `yes_bid = 1 − no_ask`,
//!   `yes_ask = 1 − no_bid`, `yes_last = 1 − no_last`.
//! - Book arrays are sorted worst-to-best: best level is the **last** element.
//! - Prices are decimal strings in `[0, 1]`, possibly without a leading zero
//!   (`".48"`); Gamma metadata mixes in JSON numbers. Both are tolerated.
//! - WSS timestamps are ms-epoch *strings*; Gamma uses ISO-8601.

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use vw_core::types::yes_from_no;
use vw_core::{Instrument, InstrumentId, RawFrame, Tick, Venue};

use crate::ConnectorEvent;

/// What the normalizer knows about one outcome token.
#[derive(Debug, Clone)]
struct TokenInfo {
    condition_id: String,
    /// Whether this token is the market's yes-leg (see module docs).
    yes_leg: bool,
}

/// Stateful Polymarket normalizer: token-id → market mapping learned from
/// Gamma discovery frames, then applied to WSS frames.
#[derive(Debug, Default)]
pub struct Normalizer {
    tokens: HashMap<String, TokenInfo>,
    /// Yes-leg token ids in first-seen order (the WSS subscribe list).
    yes_tokens: Vec<String>,
}

impl Normalizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Yes-leg token ids of every market seen so far, in first-seen order —
    /// the connector's WSS subscribe list (D10: one subscription per market).
    pub fn yes_tokens(&self) -> &[String] {
        &self.yes_tokens
    }

    /// Normalize one raw Polymarket frame (live or replayed) into events.
    ///
    /// Dispatches on shape:
    /// - Gamma market object (has `conditionId`) → [`ConnectorEvent::Instrument`]
    ///   + snapshot [`ConnectorEvent::Tick`], and registers its tokens.
    /// - WSS `book` / `price_change` / `last_trade_price` → ticks for known
    ///   tokens.
    /// - JSON arrays (the WSS server may batch events) → each element in turn.
    /// - Anything else (`PONG`, `tick_size_change`, unknown) → empty.
    pub fn normalize_frame(&mut self, frame: &RawFrame, seq: &mut u64) -> Vec<ConnectorEvent> {
        self.normalize_value(&frame.raw_frame, frame.recv_ts, seq)
    }

    fn normalize_value(
        &mut self,
        raw: &Value,
        recv_ts: DateTime<Utc>,
        seq: &mut u64,
    ) -> Vec<ConnectorEvent> {
        if let Some(events) = raw.as_array() {
            return events
                .iter()
                .flat_map(|e| self.normalize_value(e, recv_ts, seq))
                .collect();
        }
        if raw.get("conditionId").is_some() {
            return self.gamma_market(raw, recv_ts, seq);
        }
        match raw.get("event_type").and_then(Value::as_str) {
            Some("book") => self
                .book_event(raw, recv_ts, seq)
                .into_iter()
                .map(ConnectorEvent::Tick)
                .collect(),
            Some("price_change") => self.price_change_event(raw, recv_ts, seq),
            Some("last_trade_price") => self
                .last_trade_price_event(raw, recv_ts, seq)
                .into_iter()
                .map(ConnectorEvent::Tick)
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Gamma REST market object → Instrument + snapshot tick, registering the
    /// market's outcome tokens for later WSS attribution.
    fn gamma_market(
        &mut self,
        market: &Value,
        recv_ts: DateTime<Utc>,
        seq: &mut u64,
    ) -> Vec<ConnectorEvent> {
        let Some(condition_id) = market.get("conditionId").and_then(Value::as_str) else {
            return Vec::new();
        };
        // `clobTokenIds` / `outcomes` are JSON-encoded strings *inside* JSON
        // (research §1.1): double-decode.
        let token_ids = double_decoded_strings(market, "clobTokenIds");
        let outcomes = double_decoded_strings(market, "outcomes");
        let yes_idx = yes_index(&outcomes);
        for (i, token_id) in token_ids.iter().enumerate() {
            let yes_leg = i == yes_idx;
            if self
                .tokens
                .insert(
                    token_id.clone(),
                    TokenInfo {
                        condition_id: condition_id.to_string(),
                        yes_leg,
                    },
                )
                .is_none()
                && yes_leg
            {
                self.yes_tokens.push(token_id.clone());
            }
        }

        let id = InstrumentId::new(Venue::Polymarket, condition_id);
        let instrument = Instrument {
            id: id.clone(),
            venue: Venue::Polymarket,
            title: market
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or(condition_id)
                .to_string(),
            description: market
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            close_time: rfc3339(market, "endDate"),
            // No usable category on Gamma market objects; the universe is
            // tag-filtered at discovery instead (D8, M3 keys on tags).
            category: None,
            raw: market.clone(),
        };

        // Snapshot prices: `outcomePrices` (JSON-encoded string array, ordered
        // like `outcomes`) for last price; `bestBid`/`bestAsk` are JSON
        // numbers quoting the *first* token's book, so only use them when the
        // yes-leg is index 0 (always true for binary Yes/No markets).
        let outcome_prices = double_decoded_strings(market, "outcomePrices");
        let last_price = outcome_prices.get(yes_idx).and_then(|s| parse_price(s));
        let (yes_bid, yes_ask) = if yes_idx == 0 {
            (
                lenient_price(market.get("bestBid")),
                lenient_price(market.get("bestAsk")),
            )
        } else {
            (None, None)
        };
        *seq += 1;
        let tick = Tick {
            instrument: id,
            venue: Venue::Polymarket,
            yes_bid,
            yes_ask,
            last_price,
            venue_ts: rfc3339(market, "updatedAt"),
            recv_ts,
            seq: *seq,
        };
        vec![
            ConnectorEvent::Instrument(instrument),
            ConnectorEvent::Tick(tick),
        ]
    }

    /// WSS `book` event (full snapshot; pushed on subscribe and after trades).
    /// Arrays are sorted worst-to-best: best level is the last element.
    fn book_event(&self, raw: &Value, recv_ts: DateTime<Utc>, seq: &mut u64) -> Option<Tick> {
        let info = self.lookup(raw.get("asset_id")?.as_str()?)?;
        let best_bid = last_level_price(raw.get("bids"));
        let best_ask = last_level_price(raw.get("asks"));
        let (yes_bid, yes_ask) = orient(info.yes_leg, best_bid, best_ask);
        *seq += 1;
        Some(Tick {
            instrument: InstrumentId::new(Venue::Polymarket, &info.condition_id),
            venue: Venue::Polymarket,
            yes_bid,
            yes_ask,
            last_price: None,
            venue_ts: ms_epoch(raw.get("timestamp")),
            recv_ts,
            seq: *seq,
        })
    }

    /// WSS `price_change` event: one tick per known asset entry, using the
    /// per-asset `best_bid` / `best_ask` (self-healing top-of-book without
    /// full book reconstruction — research §2).
    fn price_change_event(
        &self,
        raw: &Value,
        recv_ts: DateTime<Utc>,
        seq: &mut u64,
    ) -> Vec<ConnectorEvent> {
        let venue_ts = ms_epoch(raw.get("timestamp"));
        let Some(changes) = raw.get("price_changes").and_then(Value::as_array) else {
            return Vec::new();
        };
        changes
            .iter()
            .filter_map(|change| {
                let info = self.lookup(change.get("asset_id")?.as_str()?)?;
                let best_bid = lenient_price(change.get("best_bid"));
                let best_ask = lenient_price(change.get("best_ask"));
                let (yes_bid, yes_ask) = orient(info.yes_leg, best_bid, best_ask);
                *seq += 1;
                Some(ConnectorEvent::Tick(Tick {
                    instrument: InstrumentId::new(Venue::Polymarket, &info.condition_id),
                    venue: Venue::Polymarket,
                    yes_bid,
                    yes_ask,
                    last_price: None,
                    venue_ts,
                    recv_ts,
                    seq: *seq,
                }))
            })
            .collect()
    }

    /// WSS `last_trade_price` event → last-price-only tick.
    fn last_trade_price_event(
        &self,
        raw: &Value,
        recv_ts: DateTime<Utc>,
        seq: &mut u64,
    ) -> Option<Tick> {
        let info = self.lookup(raw.get("asset_id")?.as_str()?)?;
        let price = lenient_price(raw.get("price"))?;
        let last_price = if info.yes_leg {
            price
        } else {
            yes_from_no(price)
        };
        *seq += 1;
        Some(Tick {
            instrument: InstrumentId::new(Venue::Polymarket, &info.condition_id),
            venue: Venue::Polymarket,
            yes_bid: None,
            yes_ask: None,
            last_price: Some(last_price),
            venue_ts: ms_epoch(raw.get("timestamp")),
            recv_ts,
            seq: *seq,
        })
    }

    fn lookup(&self, token_id: &str) -> Option<&TokenInfo> {
        let info = self.tokens.get(token_id);
        if info.is_none() {
            tracing::debug!(token_id, "WSS frame for unknown token; skipped");
        }
        info
    }
}

/// Complement NO-leg quotes into YES terms: a NO bid at `p` is a YES ask at
/// `1 − p`, and vice versa (sides swap).
fn orient(
    yes_leg: bool,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
) -> (Option<Decimal>, Option<Decimal>) {
    if yes_leg {
        (best_bid, best_ask)
    } else {
        (best_ask.map(yes_from_no), best_bid.map(yes_from_no))
    }
}

/// Index of the `"Yes"` outcome, or 0 for non-Yes/No markets (D10).
fn yes_index(outcomes: &[String]) -> usize {
    outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("yes"))
        .unwrap_or(0)
}

/// Decode a Gamma field that is a JSON-encoded string of a string array
/// (`"[\"Yes\", \"No\"]"`). Tolerates it already being a plain array.
fn double_decoded_strings(obj: &Value, key: &str) -> Vec<String> {
    let decoded;
    let arr = match obj.get(key) {
        Some(Value::String(s)) => {
            decoded = serde_json::from_str::<Value>(s).unwrap_or(Value::Null);
            &decoded
        }
        Some(v) => v,
        None => return Vec::new(),
    };
    arr.as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a price string, tolerating a missing leading zero (`".48"`).
fn parse_price(s: &str) -> Option<Decimal> {
    let s = s.trim();
    if let Some(frac) = s.strip_prefix('.') {
        return Decimal::from_str(&format!("0.{frac}")).ok();
    }
    Decimal::from_str(s).ok()
}

/// Price from a JSON string *or* number (Gamma mixes both; research §3).
fn lenient_price(v: Option<&Value>) -> Option<Decimal> {
    match v? {
        Value::String(s) => parse_price(s),
        n @ Value::Number(_) => Decimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}

/// Best price from a WSS book side: the **last** level (worst-to-best order).
fn last_level_price(side: Option<&Value>) -> Option<Decimal> {
    lenient_price(side?.as_array()?.last()?.get("price"))
}

/// ms-epoch string (or number) timestamp → UTC.
fn ms_epoch(v: Option<&Value>) -> Option<DateTime<Utc>> {
    let ms = match v? {
        Value::String(s) => s.trim().parse::<i64>().ok()?,
        n @ Value::Number(_) => n.as_i64()?,
        _ => return None,
    };
    Utc.timestamp_millis_opt(ms).single()
}

fn rfc3339(obj: &Value, key: &str) -> Option<DateTime<Utc>> {
    let s = obj.get(key)?.as_str()?;
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn frame(raw: Value) -> RawFrame {
        RawFrame {
            recv_ts: Utc::now(),
            venue: Venue::Polymarket,
            raw_frame: raw,
        }
    }

    /// REAL payload: live capture 2026-07-18 (docs/research/polymarket-api.md
    /// §1.1, `GET /markets?limit=1&active=true&closed=false&order=volume24hr`),
    /// abridged to the fields normalization reads plus a few extras.
    fn live_gamma_market() -> Value {
        serde_json::json!({
            "id": "2063129",
            "question": "Will Belete Molla be the next Prime Minister of Ethiopia?",
            "conditionId": "0x7c97f7315ac9a2e0eabe9b2b9caa8369feff95180483b5168a5274b69762690c",
            "slug": "will-belete-molla-be-the-next-prime-minister-of-ethiopia",
            "endDate": "2026-06-01T00:00:00Z",
            "description": "General elections are scheduled to be held in Ethiopia on June 1, 2026.",
            "outcomes": "[\"Yes\", \"No\"]",
            "outcomePrices": "[\"0.0035\", \"0.9965\"]",
            "active": true,
            "closed": false,
            "updatedAt": "2026-07-18T20:25:07.154811Z",
            "enableOrderBook": true,
            "orderPriceMinTickSize": 0.001,
            "volume24hr": 5206789.542,
            "clobTokenIds": "[\"85367286745806857961178482075931972831841231758328346969840810630055458089640\", \"40069008842150598748086988698459627032664680273804858199848489101623205239948\"]",
            "acceptingOrders": true,
            "negRisk": true,
            "bestBid": 0.003,
            "bestAsk": 0.004,
            "lastTradePrice": 0.003,
            "spread": 0.001
        })
    }

    const YES_TOKEN: &str =
        "85367286745806857961178482075931972831841231758328346969840810630055458089640";
    const NO_TOKEN: &str =
        "40069008842150598748086988698459627032664680273804858199848489101623205239948";
    const CONDITION: &str = "0x7c97f7315ac9a2e0eabe9b2b9caa8369feff95180483b5168a5274b69762690c";

    /// REAL payload: live WSS capture 2026-07-18
    /// (docs/research/polymarket-api.md §2, book snapshot on subscribe,
    /// truncated levels). Best bid/ask are the LAST array elements.
    fn live_ws_book() -> Value {
        serde_json::json!({
            "market": CONDITION,
            "asset_id": YES_TOKEN,
            "timestamp": "1784392420471",
            "hash": "1243065dd822da900bfa02f607e50a8b416ccc5e",
            "bids": [
                {"price": "0.001", "size": "21689.85"},
                {"price": "0.002", "size": "14481.32"},
                {"price": "0.003", "size": "10000"}
            ],
            "asks": [
                {"price": "0.999", "size": "92.71"},
                {"price": "0.998", "size": "18.2"},
                {"price": "0.004", "size": "5000"}
            ],
            "event_type": "book"
        })
    }

    /// REAL payload: live WSS capture 2026-07-18
    /// (docs/research/polymarket-api.md §2, `price_change` with entries for
    /// both legs of the market).
    fn live_ws_price_change() -> Value {
        serde_json::json!({
            "market": CONDITION,
            "price_changes": [
                {"asset_id": NO_TOKEN, "price": "0.37", "size": "850", "side": "BUY",
                 "hash": "abcad7e58703565112ec4e45694d46a14d1a631c",
                 "best_bid": "0.996", "best_ask": "0.997"},
                {"asset_id": YES_TOKEN, "price": "0.63", "size": "850", "side": "SELL",
                 "hash": "bdd0f19c10b07334d1f25012b7d1dc89255a7178",
                 "best_bid": "0.003", "best_ask": "0.004"}
            ],
            "timestamp": "1784392423403",
            "event_type": "price_change"
        })
    }

    /// A normalizer that has already seen the discovery frame.
    fn seeded() -> (Normalizer, u64) {
        let mut n = Normalizer::new();
        let mut seq = 0;
        n.normalize_frame(&frame(live_gamma_market()), &mut seq);
        (n, seq)
    }

    #[test]
    fn gamma_market_yields_instrument_and_snapshot_tick() {
        let mut n = Normalizer::new();
        let mut seq = 0;
        let events = n.normalize_frame(&frame(live_gamma_market()), &mut seq);
        assert_eq!(events.len(), 2);
        let ConnectorEvent::Instrument(inst) = &events[0] else {
            panic!("expected instrument");
        };
        assert_eq!(inst.id.0, format!("polymarket:{CONDITION}"));
        assert_eq!(inst.venue, Venue::Polymarket);
        assert!(inst.title.starts_with("Will Belete Molla"));
        assert!(inst
            .description
            .as_deref()
            .unwrap()
            .contains("General elections"));
        assert_eq!(
            inst.close_time.unwrap(),
            Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap()
        );
        // raw payload preserved verbatim, including the double-encoded fields
        assert_eq!(inst.raw["outcomePrices"], "[\"0.0035\", \"0.9965\"]");

        let ConnectorEvent::Tick(tick) = &events[1] else {
            panic!("expected tick");
        };
        assert_eq!(tick.instrument.0, format!("polymarket:{CONDITION}"));
        assert_eq!(tick.yes_bid, Some(dec!(0.003)));
        assert_eq!(tick.yes_ask, Some(dec!(0.004)));
        assert_eq!(tick.last_price, Some(dec!(0.0035)));
        assert_eq!(tick.seq, 1);
        assert_eq!(
            tick.venue_ts.unwrap().to_rfc3339(),
            "2026-07-18T20:25:07.154811+00:00"
        );
        // tokens registered; yes-leg (outcome "Yes" = index 0) is subscribable
        assert_eq!(n.yes_tokens(), &[YES_TOKEN.to_string()]);
    }

    #[test]
    fn ws_book_takes_best_from_array_end() {
        let (mut n, mut seq) = seeded();
        let events = n.normalize_frame(&frame(live_ws_book()), &mut seq);
        assert_eq!(events.len(), 1);
        let ConnectorEvent::Tick(tick) = &events[0] else {
            panic!("expected tick");
        };
        assert_eq!(tick.instrument.0, format!("polymarket:{CONDITION}"));
        // best = LAST element, not first
        assert_eq!(tick.yes_bid, Some(dec!(0.003)));
        assert_eq!(tick.yes_ask, Some(dec!(0.004)));
        assert_eq!(tick.last_price, None);
        assert_eq!(tick.venue_ts.unwrap().timestamp_millis(), 1784392420471);
        assert_eq!(tick.seq, 2);
    }

    #[test]
    fn ws_price_change_complements_no_leg() {
        let (mut n, mut seq) = seeded();
        let events = n.normalize_frame(&frame(live_ws_price_change()), &mut seq);
        assert_eq!(events.len(), 2, "one tick per known asset entry");
        // First entry is the NO token quoting 0.996/0.997 → complemented.
        let ConnectorEvent::Tick(no_leg) = &events[0] else {
            panic!("expected tick");
        };
        assert_eq!(no_leg.instrument.0, format!("polymarket:{CONDITION}"));
        assert_eq!(no_leg.yes_bid, Some(dec!(0.003))); // 1 − 0.997
        assert_eq!(no_leg.yes_ask, Some(dec!(0.004))); // 1 − 0.996
                                                       // Second entry is the YES token: passed through.
        let ConnectorEvent::Tick(yes_leg) = &events[1] else {
            panic!("expected tick");
        };
        assert_eq!(yes_leg.yes_bid, Some(dec!(0.003)));
        assert_eq!(yes_leg.yes_ask, Some(dec!(0.004)));
        assert_eq!(yes_leg.venue_ts.unwrap().timestamp_millis(), 1784392423403);
        assert!(yes_leg.seq > no_leg.seq, "seq monotonic within one frame");
    }

    /// DOC-SOURCED payload: the market-channel doc's `book` example with
    /// leading-zero-less prices (".48") — parser must accept both forms
    /// (docs/research/polymarket-api.md §2).
    #[test]
    fn doc_book_prices_without_leading_zero_parse() {
        let market = serde_json::json!({
            "conditionId": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
            "question": "doc example",
            "outcomes": "[\"Yes\", \"No\"]",
            "clobTokenIds": "[\"65818619657568813474341868652308942079804919287380422192892211131408793125422\", \"71321045679252212594626385532706912750332728571942532289631379312455583992563\"]"
        });
        let book = serde_json::json!({
            "event_type": "book",
            "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
            "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
            "bids": [{"price": ".48", "size": "30"}, {"price": ".49", "size": "20"}, {"price": ".50", "size": "15"}],
            "asks": [{"price": ".52", "size": "25"}, {"price": ".53", "size": "60"}, {"price": ".54", "size": "10"}],
            "timestamp": "123456789000",
            "hash": "0x0...."
        });
        let mut n = Normalizer::new();
        let mut seq = 0;
        n.normalize_frame(&frame(market), &mut seq);
        let events = n.normalize_frame(&frame(book), &mut seq);
        let ConnectorEvent::Tick(tick) = &events[0] else {
            panic!("expected tick: {events:?}");
        };
        assert_eq!(tick.yes_bid, Some(dec!(0.50)));
        assert_eq!(tick.yes_ask, Some(dec!(0.54)));
    }

    /// SYNTHETIC: non-Yes/No outcomes (sports markets use team names —
    /// research §1.1); token index 0 becomes the yes-leg by convention, and
    /// Gamma bestBid/bestAsk apply since the leg is index 0.
    #[test]
    fn non_yes_no_market_uses_first_outcome_as_yes_leg() {
        let market = serde_json::json!({
            "conditionId": "0xaaa",
            "question": "T1 vs Karmine Corp",
            "outcomes": "[\"T1\", \"Karmine Corp\"]",
            "outcomePrices": "[\"0.62\", \"0.38\"]",
            "clobTokenIds": "[\"111\", \"222\"]",
            "bestBid": 0.61,
            "bestAsk": 0.63
        });
        let mut n = Normalizer::new();
        let mut seq = 0;
        let events = n.normalize_frame(&frame(market), &mut seq);
        let ConnectorEvent::Tick(tick) = &events[1] else {
            panic!("expected tick");
        };
        assert_eq!(tick.last_price, Some(dec!(0.62)));
        assert_eq!(tick.yes_bid, Some(dec!(0.61)));
        assert_eq!(n.yes_tokens(), &["111".to_string()]);
        // A frame for the second token is complemented like a NO leg.
        let book = serde_json::json!({
            "event_type": "book",
            "asset_id": "222",
            "bids": [{"price": "0.36", "size": "1"}],
            "asks": [{"price": "0.40", "size": "1"}],
            "timestamp": "1784392420471"
        });
        let events = n.normalize_frame(&frame(book), &mut seq);
        let ConnectorEvent::Tick(tick) = &events[0] else {
            panic!("expected tick");
        };
        assert_eq!(tick.yes_bid, Some(dec!(0.60))); // 1 − 0.40
        assert_eq!(tick.yes_ask, Some(dec!(0.64))); // 1 − 0.36
    }

    #[test]
    fn unknown_tokens_and_noise_are_skipped_without_consuming_seq() {
        let mut n = Normalizer::new();
        let mut seq = 7;
        // book for a token never seen in discovery
        assert!(n
            .normalize_frame(&frame(live_ws_book()), &mut seq)
            .is_empty());
        // tick_size_change (research §2 event type we don't consume)
        let tsc = serde_json::json!({"event_type": "tick_size_change", "asset_id": "1", "new_tick_size": "0.001"});
        assert!(n.normalize_frame(&frame(tsc), &mut seq).is_empty());
        // garbage / PONG-shaped values
        assert!(n
            .normalize_frame(&frame(serde_json::json!("PONG")), &mut seq)
            .is_empty());
        assert!(n
            .normalize_frame(&frame(serde_json::json!({"hello": "world"})), &mut seq)
            .is_empty());
        assert_eq!(seq, 7, "ignored frames must not consume sequence numbers");
    }

    /// The WSS server may batch events in a JSON array (research §2).
    #[test]
    fn array_wrapped_events_normalize_element_wise() {
        let (mut n, mut seq) = seeded();
        let batch = serde_json::json!([live_ws_book(), live_ws_price_change()]);
        let events = n.normalize_frame(&frame(batch), &mut seq);
        assert_eq!(events.len(), 3); // 1 book + 2 price_change entries
        assert!(events.iter().all(|e| matches!(e, ConnectorEvent::Tick(_))));
    }

    #[test]
    fn price_parsing_tolerates_all_observed_forms() {
        assert_eq!(parse_price(".48"), Some(dec!(0.48)));
        assert_eq!(parse_price("0.996"), Some(dec!(0.996)));
        assert_eq!(parse_price("1"), Some(dec!(1)));
        assert_eq!(parse_price("garbage"), None);
        assert_eq!(
            lenient_price(Some(&serde_json::json!(0.003))),
            Some(dec!(0.003))
        );
        assert_eq!(
            lenient_price(Some(&serde_json::json!(".37"))),
            Some(dec!(0.37))
        );
        assert_eq!(lenient_price(Some(&serde_json::json!(null))), None);
        assert_eq!(lenient_price(None), None);
    }
}
