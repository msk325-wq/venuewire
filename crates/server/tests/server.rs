//! End-to-end tests: real axum server on an ephemeral loopback port, real
//! HTTP client (reqwest) and WS client (tokio-tungstenite).

use futures_util::{SinkExt, StreamExt};
use rust_decimal_macros::dec;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use vw_core::config::DivergenceConfig;
use vw_core::{DivergenceEvent, InstrumentId, MatchedMarket, Tick, Venue};
use vw_server::{PublishHandle, Server, ServerMetrics, ServerOptions};
use vw_state::{PipelineState, SystemClock};

const KALSHI_ID: &str = "kalshi:FED-26JUL";
const POLY_ID: &str = "polymarket:0xabc";
const MATCH_ID: &str = "fed-26jul";

fn tick(instrument: &str, venue: Venue, seq: u64) -> Tick {
    Tick {
        instrument: InstrumentId(instrument.to_string()),
        venue,
        yes_bid: Some(dec!(0.40)),
        yes_ask: Some(dec!(0.44)),
        last_price: None,
        venue_ts: None,
        recv_ts: chrono::Utc::now(),
        seq,
    }
}

fn divergence() -> DivergenceEvent {
    DivergenceEvent {
        match_id: MATCH_ID.to_string(),
        spread: dec!(0.05),
        legs: vec![
            (InstrumentId(KALSHI_ID.to_string()), dec!(0.42)),
            (InstrumentId(POLY_ID.to_string()), dec!(0.47)),
        ],
        detected_at: chrono::Utc::now(),
    }
}

/// A PipelineState with two quoted instruments joined by one match.
fn seeded_state() -> Arc<PipelineState> {
    let matches = vec![MatchedMarket {
        match_id: MATCH_ID.to_string(),
        legs: vec![
            InstrumentId(KALSHI_ID.to_string()),
            InstrumentId(POLY_ID.to_string()),
        ],
        confidence: vw_core::MatchConfidence::High,
        method: vw_core::MatchMethod::Rule,
    }];
    let state = Arc::new(PipelineState::new(
        DivergenceConfig::default(),
        &matches,
        Arc::new(SystemClock),
        None,
    ));
    state.apply(&tick(KALSHI_ID, Venue::Kalshi, 1));
    state.apply(&tick(POLY_ID, Venue::Polymarket, 1));
    state
}

async fn spawn_server(options: ServerOptions) -> (SocketAddr, PublishHandle, Arc<ServerMetrics>) {
    let server = Server::new(seeded_state(), options);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = server.router;
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (addr, server.publish, server.metrics)
}

type WsClient = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: SocketAddr, token: Option<&str>) -> WsClient {
    let mut req = format!("ws://{addr}/ws").into_client_request().unwrap();
    if let Some(token) = token {
        req.headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
    }
    let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws
}

async fn ws_send(ws: &mut WsClient, msg: &str) {
    ws.send(Message::Text(msg.to_string())).await.unwrap();
}

/// Next text frame, parsed. Panics after 5s to keep failures loud.
async fn ws_recv(ws: &mut WsClient) -> serde_json::Value {
    let deadline = tokio::time::Duration::from_secs(5);
    loop {
        let msg = tokio::time::timeout(deadline, ws.next())
            .await
            .expect("timed out waiting for ws frame")
            .expect("stream ended")
            .expect("ws error");
        if let Message::Text(text) = msg {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

async fn subscribe(ws: &mut WsClient, topics: &[&str]) {
    let op = serde_json::json!({ "op": "subscribe", "topics": topics });
    ws_send(ws, &op.to_string()).await;
    let ack = ws_recv(ws).await;
    assert_eq!(ack["op"], "subscribed");
}

// ---------------------------------------------------------------- REST

#[tokio::test]
async fn health_reports_pipeline_counters() {
    let (addr, _publish, _metrics) = spawn_server(ServerOptions::default()).await;
    let body: serde_json::Value = reqwest::get(format!("http://{addr}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["instruments"], 2);
    assert_eq!(body["matches"], 1);
    assert_eq!(body["ticks_accepted"], 2);
    assert_eq!(body["ticks_rejected"], 0);
    assert_eq!(body["ws_clients"], 0);
}

#[tokio::test]
async fn instruments_and_quote_snapshots() {
    let (addr, _publish, _metrics) = spawn_server(ServerOptions::default()).await;

    let list: serde_json::Value = reqwest::get(format!("http://{addr}/instruments"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let list = list.as_array().unwrap();
    assert_eq!(list.len(), 2);
    // sorted by instrument id
    assert_eq!(list[0]["instrument"], KALSHI_ID);
    assert_eq!(list[1]["instrument"], POLY_ID);
    assert_eq!(list[0]["yes_bid"], "0.40");
    assert_eq!(list[0]["mid"], "0.42");

    let quote: serde_json::Value = reqwest::get(format!("http://{addr}/quotes/{KALSHI_ID}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(quote["instrument"], KALSHI_ID);
    assert_eq!(quote["yes_ask"], "0.44");
    assert_eq!(quote["seq"], 1);

    let missing = reqwest::get(format!("http://{addr}/quotes/kalshi:NOPE"))
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn match_snapshots() {
    let (addr, _publish, _metrics) = spawn_server(ServerOptions::default()).await;

    let all: serde_json::Value = reqwest::get(format!("http://{addr}/matches"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let all = all.as_array().unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0]["match_id"], MATCH_ID);
    assert_eq!(all[0]["legs"].as_array().unwrap().len(), 2);
    assert_eq!(all[0]["best_yes_bid"], "0.40");
    assert_eq!(all[0]["best_yes_ask"], "0.44");

    let view: serde_json::Value = reqwest::get(format!("http://{addr}/matches/{MATCH_ID}/view"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view["match_id"], MATCH_ID);
    assert_eq!(view["legs"][0]["mid"], "0.42");

    let missing = reqwest::get(format!("http://{addr}/matches/nope/view"))
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
}

#[tokio::test]
async fn metrics_exposes_prometheus_text() {
    let (addr, _publish, _metrics) = spawn_server(ServerOptions::default()).await;
    let resp = reqwest::get(format!("http://{addr}/metrics"))
        .await
        .unwrap();
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
    let text = resp.text().await.unwrap();
    assert!(text.contains("vw_ws_clients 0"));
    assert!(text.contains("vw_ws_lagged_total 0"));
}

// ---------------------------------------------------------------- auth

#[tokio::test]
async fn auth_disabled_allows_everything() {
    let (addr, _publish, _metrics) = spawn_server(ServerOptions::default()).await;
    for path in ["/health", "/instruments", "/matches", "/metrics"] {
        let resp = reqwest::get(format!("http://{addr}{path}")).await.unwrap();
        assert_eq!(resp.status(), 200, "{path}");
    }
    // WS connects without a token
    let mut ws = ws_connect(addr, None).await;
    subscribe(&mut ws, &["ticks:*"]).await;
}

#[tokio::test]
async fn auth_enforced_when_token_configured() {
    let options = ServerOptions {
        auth_token: "s3cret".into(),
        ..ServerOptions::default()
    };
    let (addr, _publish, _metrics) = spawn_server(options).await;
    let client = reqwest::Client::new();

    // health and metrics stay open
    for path in ["/health", "/metrics"] {
        let resp = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "{path}");
    }

    // everything else requires the exact token
    let no_header = client
        .get(format!("http://{addr}/instruments"))
        .send()
        .await
        .unwrap();
    assert_eq!(no_header.status(), 401);

    let wrong = client
        .get(format!("http://{addr}/instruments"))
        .header("Authorization", "Bearer wrong")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong.status(), 401);

    let not_bearer = client
        .get(format!("http://{addr}/instruments"))
        .header("Authorization", "s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(not_bearer.status(), 401);

    let right = client
        .get(format!("http://{addr}/instruments"))
        .header("Authorization", "Bearer s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(right.status(), 200);

    // WS: rejected without token, accepted with it
    let req = format!("ws://{addr}/ws").into_client_request().unwrap();
    let err = tokio_tungstenite::connect_async(req).await.unwrap_err();
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => {
            assert_eq!(resp.status(), 401);
        }
        other => panic!("expected HTTP 401 rejection, got {other:?}"),
    }
    let mut ws = ws_connect(addr, Some("s3cret")).await;
    subscribe(&mut ws, &["ticks:*"]).await;
}

// ---------------------------------------------------------------- websocket

#[tokio::test]
async fn ws_receives_subscribed_ticks_and_divergences() {
    let (addr, publish, metrics) = spawn_server(ServerOptions::default()).await;
    let mut ws = ws_connect(addr, None).await;
    subscribe(&mut ws, &["ticks:*", "divergence:*"]).await;
    assert_eq!(metrics.ws_clients(), 1);

    publish.publish_tick(tick(KALSHI_ID, Venue::Kalshi, 2));
    let frame = ws_recv(&mut ws).await;
    assert_eq!(frame["op"], "tick");
    assert_eq!(frame["data"]["instrument"], KALSHI_ID);
    assert_eq!(frame["data"]["yes_bid"], "0.40");
    assert_eq!(frame["data"]["seq"], 2);

    publish.publish_divergence(divergence());
    let frame = ws_recv(&mut ws).await;
    assert_eq!(frame["op"], "divergence");
    assert_eq!(frame["data"]["match_id"], MATCH_ID);
    assert_eq!(frame["data"]["spread"], "0.05");

    drop(ws);
    // gauge decrements once the server notices the close
    for _ in 0..50 {
        if metrics.ws_clients() == 0 {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    assert_eq!(metrics.ws_clients(), 0);
}

#[tokio::test]
async fn ws_filters_by_venue_and_instrument() {
    let (addr, publish, _metrics) = spawn_server(ServerOptions::default()).await;

    let mut venue_ws = ws_connect(addr, None).await;
    subscribe(&mut venue_ws, &["ticks:kalshi:*"]).await;
    let mut exact_ws = ws_connect(addr, None).await;
    subscribe(&mut exact_ws, &[&format!("ticks:{KALSHI_ID}")]).await;

    // Neither subscriber matches the polymarket tick; both match the kalshi
    // one, so the *first* frame each receives must be the kalshi tick.
    publish.publish_tick(tick(POLY_ID, Venue::Polymarket, 2));
    publish.publish_tick(tick(KALSHI_ID, Venue::Kalshi, 2));

    for ws in [&mut venue_ws, &mut exact_ws] {
        let frame = ws_recv(ws).await;
        assert_eq!(frame["op"], "tick");
        assert_eq!(frame["data"]["instrument"], KALSHI_ID);
    }
}

#[tokio::test]
async fn ws_unsubscribe_stops_delivery() {
    let (addr, publish, _metrics) = spawn_server(ServerOptions::default()).await;
    let mut ws = ws_connect(addr, None).await;
    subscribe(&mut ws, &["ticks:*", "divergence:*"]).await;

    publish.publish_tick(tick(KALSHI_ID, Venue::Kalshi, 2));
    assert_eq!(ws_recv(&mut ws).await["op"], "tick");

    let op = serde_json::json!({ "op": "unsubscribe", "topics": ["ticks:*"] });
    ws_send(&mut ws, &op.to_string()).await;
    let ack = ws_recv(&mut ws).await;
    assert_eq!(ack["op"], "unsubscribed");
    assert_eq!(ack["topics"], serde_json::json!(["divergence:*"]));

    // The tick published after unsubscribing is filtered; the divergence
    // published after it still arrives — proving the tick was skipped.
    publish.publish_tick(tick(KALSHI_ID, Venue::Kalshi, 3));
    publish.publish_divergence(divergence());
    let frame = ws_recv(&mut ws).await;
    assert_eq!(frame["op"], "divergence");
}

#[tokio::test]
async fn ws_invalid_op_gets_error_frame() {
    let (addr, _publish, _metrics) = spawn_server(ServerOptions::default()).await;
    let mut ws = ws_connect(addr, None).await;
    ws_send(&mut ws, "{\"op\":\"nope\"}").await;
    let frame = ws_recv(&mut ws).await;
    assert_eq!(frame["op"], "error");
}

/// Slow-consumer semantics (D-M5-1): a client that falls behind the broadcast
/// buffer gets one lagged frame with the missed count, then delivery resumes
/// from the oldest retained event — the publisher is never blocked.
#[tokio::test]
async fn ws_lagged_client_is_told_and_continues() {
    let options = ServerOptions {
        broadcast_capacity: 4,
        ..ServerOptions::default()
    };
    let (addr, publish, metrics) = spawn_server(options).await;
    let mut ws = ws_connect(addr, None).await;
    subscribe(&mut ws, &["ticks:*"]).await;

    // Current-thread runtime: this synchronous burst runs while the session
    // task is parked, so the session's receiver (capacity 4) must lag.
    for seq in 2..=21 {
        publish.publish_tick(tick(KALSHI_ID, Venue::Kalshi, seq));
    }

    let frame = ws_recv(&mut ws).await;
    assert_eq!(frame["op"], "lagged");
    assert_eq!(frame["missed"], 16); // 20 published - 4 retained

    // delivery continues from the oldest retained tick (seq 18..21)
    for seq in 18..=21 {
        let frame = ws_recv(&mut ws).await;
        assert_eq!(frame["op"], "tick");
        assert_eq!(frame["data"]["seq"], seq);
    }

    // and the session is still healthy for fresh events
    publish.publish_tick(tick(KALSHI_ID, Venue::Kalshi, 22));
    let frame = ws_recv(&mut ws).await;
    assert_eq!(frame["op"], "tick");
    assert_eq!(frame["data"]["seq"], 22);

    assert_eq!(metrics.ws_lagged_total(), 16);
}
