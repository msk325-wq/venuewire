//! Capture raw venue frames (pre-normalization) to NDJSON fixture files for
//! deterministic replay in tests, demos, and benchmarks (spec §8).
//!
//! One file per venue per session: `{dir}/{venue}-{session_utc}.ndjson`, one
//! `{"recv_ts", "venue", "raw_frame"}` object per line ([`vw_core::RawFrame`]'s
//! serde form). Connectors feed the recorder through an optional raw-frame tap
//! (`mpsc::Sender<RawFrame>`), so recording is zero-cost when disabled and can
//! never backpressure ingest (connectors `try_send` and drop on full).
//!
//! Writes are buffered (`BufWriter`) with a periodic safety flush; the final
//! flush happens on [`Recorder::shutdown`] once all senders are dropped.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use vw_core::{RawFrame, Venue};

/// Buffered frames the tap may hold before connectors start dropping frames
/// (recording is best-effort by design).
const TAP_CAPACITY: usize = 4096;

/// Periodic safety flush so a crash loses at most this much history.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct Recorder {
    path: PathBuf,
    tx: mpsc::Sender<RawFrame>,
    task: JoinHandle<anyhow::Result<u64>>,
}

impl Recorder {
    /// Start recording `venue` frames to a new session file under `dir`
    /// (created if missing). Must be called within a tokio runtime.
    pub fn start(dir: &Path, venue: Venue) -> anyhow::Result<Recorder> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating fixtures dir {}", dir.display()))?;
        let session = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        let path = dir.join(format!("{venue}-{session}.ndjson"));
        let file = File::create(&path)
            .with_context(|| format!("creating fixture file {}", path.display()))?;
        let (tx, rx) = mpsc::channel(TAP_CAPACITY);
        let task = tokio::spawn(write_loop(BufWriter::new(file), rx));
        tracing::info!(path = %path.display(), "recorder started");
        Ok(Recorder { path, tx, task })
    }

    /// The session file being written.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A tap handle to hand to a connector (`KalshiConnector::with_raw_tap`).
    pub fn tap(&self) -> mpsc::Sender<RawFrame> {
        self.tx.clone()
    }

    /// Stop accepting frames, drain the buffer, flush, and close the file.
    /// Returns the number of frames written.
    ///
    /// Any connector-held tap clones must be dropped (tasks aborted) first, or
    /// this waits for them.
    pub async fn shutdown(self) -> anyhow::Result<u64> {
        drop(self.tx);
        let frames = self.task.await.context("recorder task panicked")??;
        tracing::info!(frames, path = %self.path.display(), "recorder flushed and closed");
        Ok(frames)
    }
}

async fn write_loop(
    mut out: BufWriter<File>,
    mut rx: mpsc::Receiver<RawFrame>,
) -> anyhow::Result<u64> {
    let mut frames = 0u64;
    let mut dirty = false;
    let mut flush_tick = tokio::time::interval(FLUSH_INTERVAL);
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            frame = rx.recv() => {
                match frame {
                    Some(frame) => {
                        serde_json::to_writer(&mut out, &frame).context("serializing frame")?;
                        out.write_all(b"\n").context("writing frame")?;
                        frames += 1;
                        dirty = true;
                    }
                    // All senders dropped: final flush and exit.
                    None => break,
                }
            }
            _ = flush_tick.tick() => {
                if dirty {
                    out.flush().context("flushing recorder buffer")?;
                    dirty = false;
                }
            }
        }
    }
    out.flush().context("final recorder flush")?;
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn frame(i: i64) -> RawFrame {
        RawFrame {
            recv_ts: Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap()
                + chrono::Duration::milliseconds(i),
            venue: Venue::Kalshi,
            raw_frame: serde_json::json!({ "ticker": format!("M-{i}"), "yes_bid_dollars": "0.4200" }),
        }
    }

    #[tokio::test]
    async fn records_frames_and_roundtrips_through_serde() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = Recorder::start(dir.path(), Venue::Kalshi).unwrap();
        let path = recorder.path().to_path_buf();
        assert!(path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("kalshi-"));

        let tap = recorder.tap();
        for i in 0..25 {
            tap.send(frame(i)).await.unwrap();
        }
        drop(tap);
        assert_eq!(recorder.shutdown().await.unwrap(), 25);

        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 25);
        let back: RawFrame = serde_json::from_str(lines[7]).unwrap();
        assert_eq!(back.venue, Venue::Kalshi);
        assert_eq!(back.raw_frame["ticker"], "M-7");
        assert_eq!(back.recv_ts, frame(7).recv_ts);
    }

    #[tokio::test]
    async fn shutdown_flushes_buffered_writes() {
        let dir = tempfile::tempdir().unwrap();
        let recorder = Recorder::start(dir.path(), Venue::Kalshi).unwrap();
        let path = recorder.path().to_path_buf();
        recorder.tap().send(frame(0)).await.unwrap();
        // No sleep past the flush interval: the data may only be in BufWriter.
        recorder.shutdown().await.unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents.lines().count(), 1);
    }
}
