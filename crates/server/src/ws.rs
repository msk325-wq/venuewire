//! `GET /ws` — WebSocket streaming with per-session topic filtering.
//!
//! ## Protocol
//!
//! Client → server (JSON text frames):
//! - `{"op":"subscribe","topics":["ticks:*","ticks:kalshi:*","ticks:kalshi:FED-26JUL","divergence:*"]}`
//! - `{"op":"unsubscribe","topics":[...]}`
//!
//! Both ops may be sent any number of times; the topic set is cumulative.
//! Each op is acknowledged with `{"op":"subscribed"|"unsubscribed",
//! "topics":[<full current set, sorted>]}`.
//!
//! Server → client:
//! - `{"op":"tick","data":{...Tick}}`
//! - `{"op":"divergence","data":{...DivergenceEvent}}`
//! - `{"op":"lagged","missed":n}` — this session fell behind the broadcast
//!   buffer and missed `n` events; delivery then continues from the oldest
//!   retained event (D-M5-1).
//! - `{"op":"error","message":"..."}` — unparseable client frame.
//!
//! ## Topics
//!
//! - `ticks:*` — every tick
//! - `ticks:{venue}:*` — every tick from one venue
//! - `ticks:{instrument-id}` — one instrument, e.g. `ticks:kalshi:FED-26JUL`
//!   (instrument ids are `{venue}:{native-id}`)
//! - `divergence:*` — every divergence event
//! - `divergence:{match_id}` — one match
//!
//! Filtering happens server-side, per session, before the frame is written.

use crate::events::StreamEvent;
use crate::metrics::ServerMetrics;
use crate::AppState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::broadcast;
use vw_core::{DivergenceEvent, Tick};

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum ClientOp {
    Subscribe { topics: Vec<String> },
    Unsubscribe { topics: Vec<String> },
}

#[derive(Debug, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum OutFrame<'a> {
    Tick { data: &'a Tick },
    Divergence { data: &'a DivergenceEvent },
    Lagged { missed: u64 },
    Subscribed { topics: Vec<&'a String> },
    Unsubscribed { topics: Vec<&'a String> },
    Error { message: String },
}

fn frame(out: &OutFrame<'_>) -> Message {
    Message::Text(
        serde_json::to_string(out)
            .expect("outbound frame serialization cannot fail")
            .into(),
    )
}

/// Does `topic` select `event`? See module docs for the topic grammar.
fn topic_matches(topic: &str, event: &StreamEvent) -> bool {
    match event {
        StreamEvent::Tick(t) => {
            let Some(rest) = topic.strip_prefix("ticks:") else {
                return false;
            };
            if rest == "*" {
                return true;
            }
            if let Some(venue) = rest.strip_suffix(":*") {
                return t.venue.as_str() == venue;
            }
            rest == t.instrument.0
        }
        StreamEvent::Divergence(d) => {
            let Some(rest) = topic.strip_prefix("divergence:") else {
                return false;
            };
            rest == "*" || rest == d.match_id
        }
    }
}

fn wants(topics: &HashSet<String>, event: &StreamEvent) -> bool {
    topics.iter().any(|t| topic_matches(t, event))
}

/// Decrements `vw_ws_clients` however the session ends.
struct ClientGauge(Arc<ServerMetrics>);

impl ClientGauge {
    fn new(metrics: Arc<ServerMetrics>) -> Self {
        metrics.ws_clients.inc();
        Self(metrics)
    }
}

impl Drop for ClientGauge {
    fn drop(&mut self) {
        self.0.ws_clients.dec();
    }
}

pub(crate) async fn ws_handler(State(app): State<AppState>, ws: WebSocketUpgrade) -> Response {
    // Subscribe before the handshake completes so events published during the
    // upgrade are not missed.
    let rx = app.events.subscribe();
    ws.on_upgrade(move |socket| session(socket, app, rx))
}

async fn session(mut socket: WebSocket, app: AppState, mut rx: broadcast::Receiver<StreamEvent>) {
    let _gauge = ClientGauge::new(Arc::clone(&app.metrics));
    let mut topics: HashSet<String> = HashSet::new();
    loop {
        tokio::select! {
            msg = socket.recv() => {
                let out = match msg {
                    None | Some(Err(_)) => break,
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Text(text))) => match serde_json::from_str(text.as_str()) {
                        Ok(ClientOp::Subscribe { topics: add }) => {
                            topics.extend(add);
                            let mut all: Vec<&String> = topics.iter().collect();
                            all.sort();
                            OutFrame::Subscribed { topics: all }
                        }
                        Ok(ClientOp::Unsubscribe { topics: remove }) => {
                            for t in &remove {
                                topics.remove(t);
                            }
                            let mut all: Vec<&String> = topics.iter().collect();
                            all.sort();
                            OutFrame::Unsubscribed { topics: all }
                        }
                        Err(e) => OutFrame::Error {
                            message: format!("invalid op: {e}"),
                        },
                    },
                    // axum answers pings automatically; ignore other frames.
                    Some(Ok(_)) => continue,
                };
                if socket.send(frame(&out)).await.is_err() {
                    break;
                }
            }
            event = rx.recv() => match event {
                Ok(event) => {
                    if !wants(&topics, &event) {
                        continue;
                    }
                    let out = match &event {
                        StreamEvent::Tick(t) => OutFrame::Tick { data: t },
                        StreamEvent::Divergence(d) => OutFrame::Divergence { data: d },
                    };
                    if socket.send(frame(&out)).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    // Never backpressure ingest for a slow consumer: report
                    // the gap and keep streaming (D-M5-1).
                    app.metrics.ws_lagged_total.inc_by(missed);
                    tracing::debug!(missed, "ws client lagged broadcast buffer");
                    if socket.send(frame(&OutFrame::Lagged { missed })).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vw_core::{InstrumentId, Venue};

    fn tick_event(instrument: &str, venue: Venue) -> StreamEvent {
        StreamEvent::Tick(Tick {
            instrument: InstrumentId(instrument.to_string()),
            venue,
            yes_bid: None,
            yes_ask: None,
            last_price: None,
            venue_ts: None,
            recv_ts: chrono::Utc::now(),
            seq: 1,
        })
    }

    fn divergence_event(match_id: &str) -> StreamEvent {
        StreamEvent::Divergence(DivergenceEvent {
            match_id: match_id.to_string(),
            spread: rust_decimal::Decimal::ZERO,
            legs: vec![],
            detected_at: chrono::Utc::now(),
        })
    }

    #[test]
    fn tick_topic_grammar() {
        let kalshi = tick_event("kalshi:FED-26JUL", Venue::Kalshi);
        let poly = tick_event("polymarket:0xabc", Venue::Polymarket);

        assert!(topic_matches("ticks:*", &kalshi));
        assert!(topic_matches("ticks:*", &poly));

        assert!(topic_matches("ticks:kalshi:*", &kalshi));
        assert!(!topic_matches("ticks:kalshi:*", &poly));

        assert!(topic_matches("ticks:kalshi:FED-26JUL", &kalshi));
        assert!(!topic_matches("ticks:kalshi:FED-26JUL", &poly));
        assert!(!topic_matches("ticks:kalshi:OTHER", &kalshi));

        // divergence topics never match ticks and vice versa
        assert!(!topic_matches("divergence:*", &kalshi));
        assert!(!topic_matches("ticks:*", &divergence_event("m1")));
    }

    #[test]
    fn divergence_topic_grammar() {
        let d = divergence_event("fed-26jul");
        assert!(topic_matches("divergence:*", &d));
        assert!(topic_matches("divergence:fed-26jul", &d));
        assert!(!topic_matches("divergence:other", &d));
    }

    #[test]
    fn empty_topic_set_matches_nothing() {
        let topics = HashSet::new();
        assert!(!wants(&topics, &tick_event("kalshi:X", Venue::Kalshi)));
    }
}
