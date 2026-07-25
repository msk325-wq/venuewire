//! Integration tests (spec §8): replay the committed fixtures through the
//! connector pipeline and assert on the resulting event streams.
//!
//! Both fixtures were captured live via `venuewire --record` with zero
//! credentials — Kalshi against the unauthenticated REST API (degraded
//! polling mode), Polymarket against Gamma REST discovery + the
//! unauthenticated CLOB WSS market channel — so these exercise the exact
//! normalization paths the live connectors use.

use rust_decimal::Decimal;
use tokio::sync::mpsc;
use vw_connectors::{
    ConnectorEvent, ConnectorStatus, ReplayConnector, ReplaySpeed, VenueConnector,
};
use vw_core::Venue;

fn committed_fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../fixtures/committed/{name}"))
}

#[tokio::test]
async fn committed_kalshi_fixture_replays_through_pipeline() {
    let fixture = committed_fixture("kalshi-rest-sample.ndjson");
    let contents = std::fs::read_to_string(&fixture).expect("committed fixture must exist");
    let frame_count = contents.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        frame_count > 50,
        "fixture unexpectedly small: {frame_count} frames"
    );
    // Unique market tickers in the fixture = expected Instrument events
    // (replay dedups re-polled metadata like the live connector does).
    let unique_tickers: std::collections::HashSet<String> = contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| {
            serde_json::from_str::<serde_json::Value>(l).ok()?["raw_frame"]["ticker"]
                .as_str()
                .map(str::to_string)
        })
        .collect();

    let (tx, mut rx) = mpsc::channel(16_384);
    let connector = ReplayConnector::new(&fixture, ReplaySpeed::Max);
    assert_eq!(connector.venue(), Venue::Kalshi);
    let task = tokio::spawn(connector.run(tx));

    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    task.await.unwrap().unwrap();

    let mut ticks = 0usize;
    let mut instruments = 0usize;
    let mut connected = false;
    let mut disconnected = false;
    let mut last_seq = 0u64;
    for event in &events {
        match event {
            ConnectorEvent::Tick(tick) => {
                ticks += 1;
                assert_eq!(tick.venue, Venue::Kalshi);
                assert!(
                    tick.instrument.0.starts_with("kalshi:"),
                    "instrument id {:?} missing venue prefix",
                    tick.instrument.0
                );
                // Connector-local seq must be strictly monotonic.
                assert!(
                    tick.seq > last_seq,
                    "seq not monotonic: {} after {last_seq}",
                    tick.seq
                );
                last_seq = tick.seq;
                // Normalized prices are probabilities in [0, 1].
                for price in [tick.yes_bid, tick.yes_ask, tick.last_price]
                    .into_iter()
                    .flatten()
                {
                    assert!(
                        price >= Decimal::ZERO && price <= Decimal::ONE,
                        "price {price} outside [0, 1]"
                    );
                }
            }
            ConnectorEvent::Instrument(inst) => {
                instruments += 1;
                assert_eq!(inst.venue, Venue::Kalshi);
                assert!(!inst.title.is_empty());
                assert!(inst.raw.is_object(), "raw venue payload must be preserved");
            }
            ConnectorEvent::Status(ConnectorStatus::Connected) => connected = true,
            ConnectorEvent::Status(ConnectorStatus::Disconnected { .. }) => disconnected = true,
            ConnectorEvent::Status(_) => {}
        }
    }

    // Every REST market frame yields Instrument + Tick.
    assert_eq!(ticks, frame_count, "one tick per recorded REST frame");
    assert!(instruments >= 1, "at least one Instrument event");
    assert_eq!(
        instruments,
        unique_tickers.len(),
        "one Instrument event per unique market in the fixture"
    );
    assert!(connected, "replay must emit Status(Connected)");
    assert!(disconnected, "replay must emit Status(Disconnected) at EOF");
}

#[tokio::test]
async fn committed_polymarket_fixture_replays_through_pipeline() {
    let fixture = committed_fixture("polymarket-ws-sample.ndjson");
    let contents = std::fs::read_to_string(&fixture).expect("committed fixture must exist");
    let frame_count = contents.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        frame_count > 50,
        "fixture unexpectedly small: {frame_count} frames"
    );
    // Unique condition ids among Gamma discovery frames = expected Instrument
    // events (replay dedups re-discovered metadata like the live connector).
    let unique_conditions: std::collections::HashSet<String> = contents
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| {
            serde_json::from_str::<serde_json::Value>(l).ok()?["raw_frame"]["conditionId"]
                .as_str()
                .map(str::to_string)
        })
        .collect();

    let (tx, mut rx) = mpsc::channel(16_384);
    let connector = ReplayConnector::new(&fixture, ReplaySpeed::Max);
    assert_eq!(connector.venue(), Venue::Polymarket);
    let task = tokio::spawn(connector.run(tx));

    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    task.await.unwrap().unwrap();

    let mut ticks = 0usize;
    let mut instruments = 0usize;
    let mut connected = false;
    let mut disconnected = false;
    let mut last_seq = 0u64;
    for event in &events {
        match event {
            ConnectorEvent::Tick(tick) => {
                ticks += 1;
                assert_eq!(tick.venue, Venue::Polymarket);
                assert!(
                    tick.instrument.0.starts_with("polymarket:0x"),
                    "instrument id {:?} must be a venue-prefixed condition id",
                    tick.instrument.0
                );
                // Connector-local seq must be strictly monotonic.
                assert!(
                    tick.seq > last_seq,
                    "seq not monotonic: {} after {last_seq}",
                    tick.seq
                );
                last_seq = tick.seq;
                // Normalized prices are probabilities in [0, 1] — including
                // NO-leg frames complemented via 1 − p.
                for price in [tick.yes_bid, tick.yes_ask, tick.last_price]
                    .into_iter()
                    .flatten()
                {
                    assert!(
                        price >= Decimal::ZERO && price <= Decimal::ONE,
                        "price {price} outside [0, 1]"
                    );
                }
            }
            ConnectorEvent::Instrument(inst) => {
                instruments += 1;
                assert_eq!(inst.venue, Venue::Polymarket);
                assert!(!inst.title.is_empty());
                assert!(inst.raw.is_object(), "raw venue payload must be preserved");
            }
            ConnectorEvent::Status(ConnectorStatus::Connected) => connected = true,
            ConnectorEvent::Status(ConnectorStatus::Disconnected { .. }) => disconnected = true,
            ConnectorEvent::Status(_) => {}
        }
    }

    assert!(ticks > 0, "WSS frames must normalize into ticks");
    assert!(
        ticks >= frame_count - unique_conditions.len(),
        "nearly every recorded WSS frame should yield at least one tick \
         (got {ticks} ticks from {frame_count} frames)"
    );
    assert!(instruments >= 1, "at least one Instrument event");
    assert_eq!(
        instruments,
        unique_conditions.len(),
        "one Instrument event per unique market in the fixture"
    );
    assert!(connected, "replay must emit Status(Connected)");
    assert!(disconnected, "replay must emit Status(Disconnected) at EOF");
}
