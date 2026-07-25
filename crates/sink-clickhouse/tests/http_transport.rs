//! `HttpTransport` tests against a local axum stub standing in for
//! ClickHouse's HTTP interface — verifies URL/query encoding and error
//! surfacing without a live server.

use axum::extract::{Query, State};
use axum::routing::post;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use vw_sink_clickhouse::{HttpTransport, Transport, TransportError};

/// (query params, body) per captured request.
type Requests = Vec<(HashMap<String, String>, String)>;

#[derive(Debug, Clone, Default)]
struct Captured {
    requests: Arc<Mutex<Requests>>,
    fail_with_500: Arc<Mutex<bool>>,
}

async fn stub_handler(
    State(cap): State<Captured>,
    Query(params): Query<HashMap<String, String>>,
    body: String,
) -> (axum::http::StatusCode, String) {
    cap.requests.lock().unwrap().push((params, body));
    if *cap.fail_with_500.lock().unwrap() {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Code: 62. DB::Exception: Syntax error".into(),
        )
    } else {
        (axum::http::StatusCode::OK, String::new())
    }
}

async fn spawn_stub() -> (SocketAddr, Captured) {
    let cap = Captured::default();
    let app = axum::Router::new()
        .route("/", post(stub_handler))
        .with_state(cap.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, cap)
}

#[tokio::test]
async fn posts_query_param_and_jsoneachrow_body() {
    let (addr, cap) = spawn_stub().await;
    let transport =
        HttpTransport::new(&format!("http://{addr}"), "default", Duration::from_secs(2)).unwrap();
    transport
        .execute(
            "INSERT INTO ticks FORMAT JSONEachRow",
            "{\"instrument\":\"kalshi:X\"}\n{\"instrument\":\"kalshi:Y\"}".into(),
        )
        .await
        .unwrap();
    let reqs = cap.requests.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    let (params, body) = &reqs[0];
    assert_eq!(
        params.get("query").map(String::as_str),
        Some("INSERT INTO ticks FORMAT JSONEachRow")
    );
    assert_eq!(params.get("database").map(String::as_str), Some("default"));
    assert_eq!(body.lines().count(), 2);
}

#[tokio::test]
async fn surfaces_clickhouse_error_body() {
    let (addr, cap) = spawn_stub().await;
    *cap.fail_with_500.lock().unwrap() = true;
    let transport =
        HttpTransport::new(&format!("http://{addr}"), "default", Duration::from_secs(2)).unwrap();
    let err = transport
        .execute("SELECT syntax error", String::new())
        .await
        .unwrap_err();
    match err {
        TransportError::Status { code, body } => {
            assert_eq!(code, 500);
            assert!(body.contains("DB::Exception"));
        }
        other => panic!("expected Status error, got {other:?}"),
    }
}

#[tokio::test]
async fn rejects_invalid_url() {
    assert!(matches!(
        HttpTransport::new("not a url", "default", Duration::from_secs(1)),
        Err(TransportError::Url(_))
    ));
}

/// Live smoke test against a real ClickHouse at localhost:8123. Requires
/// Docker/ClickHouse; run with `cargo test -p vw-sink-clickhouse -- --ignored`.
#[tokio::test]
#[ignore = "requires a live ClickHouse at http://localhost:8123"]
async fn live_ensure_schema_and_insert() {
    use vw_sink_clickhouse::{ClickHouseSink, SinkEvent, SinkOptions};

    let options = SinkOptions {
        url: "http://localhost:8123".into(),
        flush_interval: Duration::from_millis(100),
        ..SinkOptions::default()
    };
    let (sink, tx) = ClickHouseSink::new(options).unwrap();
    sink.ensure_schema().await.unwrap();
    let metrics = sink.metrics();
    let handle = tokio::spawn(sink.run());
    tx.send(SinkEvent::Tick(vw_core::Tick {
        instrument: vw_core::InstrumentId("kalshi:LIVE-TEST".into()),
        venue: vw_core::Venue::Kalshi,
        yes_bid: Some(rust_decimal_macros::dec!(0.42)),
        yes_ask: Some(rust_decimal_macros::dec!(0.44)),
        last_price: None,
        venue_ts: None,
        recv_ts: chrono::Utc::now(),
        seq: 1,
    }))
    .await
    .unwrap();
    drop(tx);
    handle.await.unwrap();
    assert_eq!(metrics.rows_written(), 1);
    assert_eq!(metrics.flush_errors(), 0);
}
