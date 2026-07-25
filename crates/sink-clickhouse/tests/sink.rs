//! Batching-writer tests against a mock transport — no ClickHouse required.

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal_macros::dec;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use vw_core::{DivergenceEvent, InstrumentId, Tick, Venue};
use vw_sink_clickhouse::{
    ClickHouseSink, SinkEvent, SinkOptions, Transport, TransportError, DIVERGENCES_DDL, TICKS_DDL,
};

/// Records every (query, body) pair; fails all calls while `fail` is set.
#[derive(Debug, Default)]
struct MockTransport {
    calls: Mutex<Vec<(String, String)>>,
    fail: AtomicBool,
}

impl MockTransport {
    fn calls(&self) -> Vec<(String, String)> {
        self.calls.lock().unwrap().clone()
    }

    fn set_fail(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl Transport for MockTransport {
    async fn execute(&self, query: &str, body: String) -> Result<(), TransportError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(TransportError::Other("injected outage".into()));
        }
        self.calls.lock().unwrap().push((query.to_string(), body));
        Ok(())
    }
}

fn ts() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap()
}

fn tick(seq: u64) -> Tick {
    Tick {
        instrument: InstrumentId("kalshi:FED-26JUL".into()),
        venue: Venue::Kalshi,
        yes_bid: Some(dec!(0.42)),
        yes_ask: Some(dec!(0.44)),
        last_price: None,
        venue_ts: None,
        recv_ts: ts(),
        seq,
    }
}

fn divergence() -> DivergenceEvent {
    DivergenceEvent {
        match_id: "fed-26jul".into(),
        spread: dec!(0.05),
        legs: vec![
            (InstrumentId("kalshi:FED-26JUL".into()), dec!(0.40)),
            (InstrumentId("polymarket:0xabc".into()), dec!(0.45)),
        ],
        detected_at: ts(),
    }
}

fn options() -> SinkOptions {
    SinkOptions {
        flush_interval: Duration::from_millis(500),
        ..SinkOptions::default()
    }
}

/// Sink + sender + transport + spawned run task.
fn spawn_sink(
    options: SinkOptions,
) -> (
    tokio::sync::mpsc::Sender<SinkEvent>,
    Arc<MockTransport>,
    Arc<vw_sink_clickhouse::SinkMetrics>,
    tokio::task::JoinHandle<()>,
) {
    let transport = Arc::new(MockTransport::default());
    let (sink, tx) = ClickHouseSink::with_transport(options, transport.clone());
    let metrics = sink.metrics();
    let handle = tokio::spawn(sink.run());
    (tx, transport, metrics, handle)
}

/// Let the (current-thread) sink task run without advancing the clock.
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn flushes_when_batch_size_reached() {
    let (tx, transport, metrics, _handle) = spawn_sink(SinkOptions {
        max_batch_rows: 3,
        ..options()
    });
    for seq in 1..=3 {
        tx.send(SinkEvent::Tick(tick(seq))).await.unwrap();
    }
    settle().await; // no time advance: this flush is size-triggered
    let calls = transport.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "INSERT INTO ticks FORMAT JSONEachRow");
    assert_eq!(calls[0].1.lines().count(), 3);
    assert_eq!(metrics.rows_written(), 3);
    assert_eq!(metrics.flush_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn flushes_on_interval_before_batch_fills() {
    let (tx, transport, metrics, _handle) = spawn_sink(options());
    tx.send(SinkEvent::Tick(tick(1))).await.unwrap();
    tx.send(SinkEvent::Tick(tick(2))).await.unwrap();
    settle().await;
    assert!(
        transport.calls().is_empty(),
        "must not flush before the interval elapses"
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    let calls = transport.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1.lines().count(), 2);
    assert_eq!(metrics.rows_written(), 2);
}

#[tokio::test(start_paused = true)]
async fn encodes_jsoneachrow_decimals_and_timestamps() {
    let (tx, transport, _metrics, _handle) = spawn_sink(options());
    tx.send(SinkEvent::Tick(tick(7))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let calls = transport.calls();
    let row: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
    assert_eq!(row["instrument"], "kalshi:FED-26JUL");
    assert_eq!(row["venue"], "kalshi");
    assert_eq!(row["yes_bid"], "0.42"); // Decimal(9,6) as string
    assert_eq!(row["yes_ask"], "0.44");
    assert_eq!(row["last_price"], serde_json::Value::Null);
    assert_eq!(row["venue_ts"], serde_json::Value::Null);
    assert_eq!(row["recv_ts"], "2026-07-18 12:00:00.000"); // DateTime64(3)
    assert_eq!(row["seq"], 7);
}

#[tokio::test(start_paused = true)]
async fn routes_divergences_to_their_table() {
    let (tx, transport, _metrics, _handle) = spawn_sink(options());
    tx.send(SinkEvent::Divergence(divergence())).await.unwrap();
    tx.send(SinkEvent::Tick(tick(1))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let calls = transport.calls();
    assert_eq!(calls.len(), 2);
    let tick_call = calls.iter().find(|c| c.0.contains("INTO ticks")).unwrap();
    let div_call = calls
        .iter()
        .find(|c| c.0.contains("INTO divergences"))
        .unwrap();
    assert_eq!(tick_call.1.lines().count(), 1);
    let row: serde_json::Value = serde_json::from_str(&div_call.1).unwrap();
    assert_eq!(row["match_id"], "fed-26jul");
    assert_eq!(row["spread"], "0.05");
    // legs is an embedded JSON *string*
    assert!(row["legs"].is_string());
    let legs: serde_json::Value = serde_json::from_str(row["legs"].as_str().unwrap()).unwrap();
    assert_eq!(legs[0][0], "kalshi:FED-26JUL");
}

#[tokio::test(start_paused = true)]
async fn outage_buffers_then_drops_oldest_then_recovers() {
    let (tx, transport, metrics, _handle) = spawn_sink(SinkOptions {
        max_batch_rows: 2,
        max_buffer_rows: 4,
        ..options()
    });
    transport.set_fail(true);
    // 8 rows → 4 sealed batches of 2; cap of 4 rows keeps only the newest 2
    // batches (seq 5..8) and drops the oldest 2 (seq 1..4).
    for seq in 1..=8 {
        tx.send(SinkEvent::Tick(tick(seq))).await.unwrap();
    }
    settle().await;
    assert!(transport.calls().is_empty());
    assert_eq!(metrics.rows_dropped(), 4);
    assert!(metrics.flush_errors() >= 1);
    assert_eq!(metrics.rows_written(), 0);

    // Recovery: pending batches flush in order on the next interval.
    transport.set_fail(false);
    tokio::time::sleep(Duration::from_millis(600)).await;
    let calls = transport.calls();
    assert_eq!(calls.len(), 2);
    let seqs: Vec<u64> = calls
        .iter()
        .flat_map(|c| c.1.lines())
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["seq"]
                .as_u64()
                .unwrap()
        })
        .collect();
    assert_eq!(seqs, vec![5, 6, 7, 8], "oldest surviving batch first");
    assert_eq!(metrics.rows_written(), 4);
}

#[tokio::test(start_paused = true)]
async fn ingest_never_stalls_during_outage() {
    let (tx, transport, metrics, _handle) = spawn_sink(SinkOptions {
        max_batch_rows: 10,
        max_buffer_rows: 50,
        channel_capacity: 8,
        ..options()
    });
    transport.set_fail(true);
    // Far more rows than channel capacity + buffer cap: sends must all
    // complete because the sink keeps draining and drop-oldest bounds memory.
    for seq in 1..=500 {
        tx.send(SinkEvent::Tick(tick(seq))).await.unwrap();
    }
    settle().await;
    assert_eq!(metrics.rows_written(), 0);
    assert!(metrics.rows_dropped() >= 400);
}

#[tokio::test(start_paused = true)]
async fn final_flush_on_channel_close() {
    let (tx, transport, metrics, handle) = spawn_sink(options());
    tx.send(SinkEvent::Tick(tick(1))).await.unwrap();
    settle().await;
    drop(tx);
    handle.await.unwrap();
    assert_eq!(transport.calls().len(), 1);
    assert_eq!(metrics.rows_written(), 1);
}

#[tokio::test]
async fn ensure_schema_issues_both_ddls() {
    let transport = Arc::new(MockTransport::default());
    let (sink, _tx) = ClickHouseSink::with_transport(SinkOptions::default(), transport.clone());
    sink.ensure_schema().await.unwrap();
    let calls = transport.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].0, TICKS_DDL);
    assert_eq!(calls[1].0, DIVERGENCES_DDL);
    assert!(calls.iter().all(|c| c.1.is_empty()));
    assert!(TICKS_DDL.contains("CREATE TABLE IF NOT EXISTS ticks"));
    assert!(DIVERGENCES_DDL.contains("Decimal(9,6)"));
    assert!(DIVERGENCES_DDL.contains("DateTime64(3, 'UTC')"));
}

#[tokio::test(start_paused = true)]
async fn registers_metrics_on_provided_registry() {
    let registry = prometheus::Registry::new();
    let (tx, _transport, _metrics, _handle) = spawn_sink(SinkOptions {
        registry: Some(registry.clone()),
        ..options()
    });
    tx.send(SinkEvent::Tick(tick(1))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    use prometheus::Encoder as _;
    let mut buf = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&registry.gather(), &mut buf)
        .unwrap();
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("vw_clickhouse_batch_flush_seconds"));
    assert!(text.contains("vw_clickhouse_rows_written_total 1"));
    assert!(text.contains("vw_clickhouse_rows_dropped_total 0"));
    assert!(text.contains("vw_clickhouse_flush_errors_total 0"));
}
