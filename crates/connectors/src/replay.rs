//! Replay connector (spec §5.4): reads an NDJSON fixture produced by the
//! recorder and re-parses every raw frame through the **same** normalization
//! code as the live connector, preserving original inter-arrival timing
//! (scaled by a speed multiplier, or `max` = no delays).
//!
//! Timing is driven by `tokio::time`, so tests run deterministically under
//! `tokio::time::pause()`. Unlike live connectors, replay sends with
//! backpressure (`send().await`) instead of conflating: fixtures must replay
//! every event bit-for-bit for tests and benchmarks.

use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time::Instant;
use vw_core::{RawFrame, Venue};

use crate::{ConnectorEvent, ConnectorStatus, VenueConnector};

/// Replay pacing: real-time multiple, or as fast as possible.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplaySpeed {
    /// Inter-arrival gaps divided by this factor (1.0 = original timing).
    Multiplier(f64),
    /// No delays at all (throughput benchmarks).
    Max,
}

impl FromStr for ReplaySpeed {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("max") {
            return Ok(ReplaySpeed::Max);
        }
        match s.parse::<f64>() {
            Ok(x) if x > 0.0 && x.is_finite() => Ok(ReplaySpeed::Multiplier(x)),
            _ => Err(format!(
                "invalid speed {s:?}: expected a positive number or \"max\""
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplayConnector {
    path: PathBuf,
    speed: ReplaySpeed,
}

impl ReplayConnector {
    pub fn new(path: impl Into<PathBuf>, speed: ReplaySpeed) -> Self {
        ReplayConnector {
            path: path.into(),
            speed,
        }
    }
}

#[async_trait]
impl VenueConnector for ReplayConnector {
    // Fixtures are per-venue (one recorder file per venue per session); the
    // reported venue is a best-effort peek at the first frame. Dispatch to a
    // normalization path happens per-frame regardless.
    fn venue(&self) -> Venue {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|contents| {
                let line = contents.lines().find(|l| !l.trim().is_empty())?;
                Some(serde_json::from_str::<RawFrame>(line).ok()?.venue)
            })
            .unwrap_or(Venue::Kalshi)
    }

    /// Replays the fixture once, then returns `Ok(())` (unlike live
    /// connectors, a finished fixture is a normal exit).
    async fn run(self, tx: mpsc::Sender<ConnectorEvent>) -> anyhow::Result<()> {
        let contents = std::fs::read_to_string(&self.path)
            .map_err(|e| anyhow::anyhow!("reading fixture {}: {e}", self.path.display()))?;
        let mut frames = Vec::new();
        for (lineno, line) in contents.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let frame: RawFrame = serde_json::from_str(line).map_err(|e| {
                anyhow::anyhow!("fixture {} line {}: {e}", self.path.display(), lineno + 1)
            })?;
            frames.push(frame);
        }
        tracing::info!(
            fixture = %self.path.display(),
            frames = frames.len(),
            speed = ?self.speed,
            "replay starting"
        );

        if tx
            .send(ConnectorEvent::Status(ConnectorStatus::Connected))
            .await
            .is_err()
        {
            return Ok(());
        }

        let start = Instant::now();
        let first_ts = frames.first().map(|f| f.recv_ts);
        let mut seq = 0u64;
        // Polymarket normalization is stateful (token → market mapping learned
        // from discovery frames earlier in the same fixture).
        let mut polymarket = crate::polymarket::normalize::Normalizer::new();
        // Mirror the live connector's first-seen Instrument dedup: re-polled
        // REST frames re-carry metadata; only the first sighting is emitted.
        let mut known = HashSet::new();
        for frame in &frames {
            if let (ReplaySpeed::Multiplier(speed), Some(first_ts)) = (self.speed, first_ts) {
                // Schedule against the fixture timeline (sleep_until, not
                // cumulative sleeps) so pacing does not drift.
                let offset = (frame.recv_ts - first_ts)
                    .to_std()
                    .unwrap_or_default() // out-of-order timestamps: no delay
                    .div_f64(speed);
                tokio::time::sleep_until(start + offset).await;
            }
            for event in normalize(frame, &mut seq, &mut polymarket) {
                if let ConnectorEvent::Instrument(inst) = &event {
                    if !known.insert(inst.id.0.clone()) {
                        continue;
                    }
                }
                if tx.send(event).await.is_err() {
                    tracing::info!("replay stopping: event channel closed");
                    return Ok(());
                }
            }
        }

        let _ = tx
            .send(ConnectorEvent::Status(ConnectorStatus::Disconnected {
                reason: format!("fixture exhausted after {} frames", frames.len()),
            }))
            .await;
        tracing::info!(frames = frames.len(), "replay complete");
        Ok(())
    }
}

/// Dispatch a recorded frame to its venue's live normalization path.
fn normalize(
    frame: &RawFrame,
    seq: &mut u64,
    polymarket: &mut crate::polymarket::normalize::Normalizer,
) -> Vec<ConnectorEvent> {
    match frame.venue {
        Venue::Kalshi => crate::kalshi::normalize::normalize_frame(frame, seq),
        Venue::Polymarket => polymarket.normalize_frame(frame, seq),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::io::Write;

    fn fixture_line(recv_ts: chrono::DateTime<Utc>, ticker: &str, bid: &str) -> String {
        serde_json::to_string(&RawFrame {
            recv_ts,
            venue: Venue::Kalshi,
            raw_frame: serde_json::json!({
                "type": "ticker",
                "sid": 1,
                "msg": { "market_ticker": ticker, "yes_bid_dollars": bid, "ts_ms": recv_ts.timestamp_millis() }
            }),
        })
        .unwrap()
    }

    fn write_fixture(lines: &[String]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        f.flush().unwrap();
        f
    }

    #[test]
    fn speed_parses_multiplier_and_max() {
        assert_eq!("max".parse::<ReplaySpeed>().unwrap(), ReplaySpeed::Max);
        assert_eq!("MAX".parse::<ReplaySpeed>().unwrap(), ReplaySpeed::Max);
        assert_eq!(
            "2.5".parse::<ReplaySpeed>().unwrap(),
            ReplaySpeed::Multiplier(2.5)
        );
        assert!("0".parse::<ReplaySpeed>().is_err());
        assert!("-1".parse::<ReplaySpeed>().is_err());
        assert!("fast".parse::<ReplaySpeed>().is_err());
    }

    /// Deterministic timing under `tokio::time::pause`: frames 2s apart at
    /// speed 2.0 must replay 1s apart (virtual time).
    #[tokio::test(start_paused = true)]
    async fn replays_with_scaled_inter_arrival_timing() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let fixture = write_fixture(&[
            fixture_line(t0, "A", "0.10"),
            fixture_line(t0 + chrono::Duration::seconds(2), "A", "0.11"),
            fixture_line(t0 + chrono::Duration::seconds(4), "A", "0.12"),
        ]);

        let (tx, mut rx) = mpsc::channel(64);
        let connector = ReplayConnector::new(fixture.path(), ReplaySpeed::Multiplier(2.0));
        let start = Instant::now();
        let task = tokio::spawn(connector.run(tx));

        let mut tick_times = Vec::new();
        while let Some(event) = rx.recv().await {
            if matches!(event, ConnectorEvent::Tick(_)) {
                tick_times.push(start.elapsed());
            }
        }
        task.await.unwrap().unwrap();

        assert_eq!(tick_times.len(), 3);
        assert_eq!(tick_times[0], std::time::Duration::ZERO);
        assert_eq!(tick_times[1], std::time::Duration::from_secs(1));
        assert_eq!(tick_times[2], std::time::Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn max_speed_skips_all_delays() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let fixture = write_fixture(&[
            fixture_line(t0, "A", "0.10"),
            fixture_line(t0 + chrono::Duration::seconds(3600), "A", "0.11"),
        ]);

        let (tx, mut rx) = mpsc::channel(64);
        let start = Instant::now();
        ReplayConnector::new(fixture.path(), ReplaySpeed::Max)
            .run(tx)
            .await
            .unwrap();
        assert_eq!(start.elapsed(), std::time::Duration::ZERO);

        let mut ticks = 0;
        while let Some(event) = rx.recv().await {
            if matches!(event, ConnectorEvent::Tick(_)) {
                ticks += 1;
            }
        }
        assert_eq!(ticks, 2);
    }

    #[tokio::test]
    async fn emits_connected_then_data_then_disconnected() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let fixture = write_fixture(&[fixture_line(t0, "A", "0.10")]);
        let (tx, mut rx) = mpsc::channel(64);
        ReplayConnector::new(fixture.path(), ReplaySpeed::Max)
            .run(tx)
            .await
            .unwrap();

        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        assert!(matches!(
            events.first(),
            Some(ConnectorEvent::Status(ConnectorStatus::Connected))
        ));
        assert!(matches!(events.get(1), Some(ConnectorEvent::Tick(_))));
        assert!(matches!(
            events.last(),
            Some(ConnectorEvent::Status(ConnectorStatus::Disconnected { .. }))
        ));
    }

    /// Re-polled REST market frames re-emit ticks but not duplicate
    /// Instrument metadata (parity with the live connector's dedup).
    #[tokio::test]
    async fn repolled_market_frames_dedup_instruments() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let market = |bid: &str| {
            serde_json::to_string(&RawFrame {
                recv_ts: t0,
                venue: Venue::Kalshi,
                raw_frame: serde_json::json!({
                    "ticker": "KXFED-27APR-T4.25",
                    "title": "Fed above 4.25%",
                    "yes_bid_dollars": bid
                }),
            })
            .unwrap()
        };
        let fixture = write_fixture(&[market("0.27"), market("0.28")]);
        let (tx, mut rx) = mpsc::channel(64);
        ReplayConnector::new(fixture.path(), ReplaySpeed::Max)
            .run(tx)
            .await
            .unwrap();

        let mut ticks = 0;
        let mut instruments = 0;
        while let Some(event) = rx.recv().await {
            match event {
                ConnectorEvent::Tick(_) => ticks += 1,
                ConnectorEvent::Instrument(_) => instruments += 1,
                ConnectorEvent::Status(_) => {}
            }
        }
        assert_eq!(ticks, 2);
        assert_eq!(instruments, 1);
    }

    #[tokio::test]
    async fn malformed_fixture_is_a_fatal_error() {
        let fixture = write_fixture(&["not json at all".to_string()]);
        let (tx, _rx) = mpsc::channel(4);
        let err = ReplayConnector::new(fixture.path(), ReplaySpeed::Max)
            .run(tx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("line 1"));
    }
}
