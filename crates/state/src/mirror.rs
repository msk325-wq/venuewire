//! Redis write-behind mirror (spec §7.1).
//!
//! Redis is a **mirror, not the source of truth**. The hot path only marks
//! dirty state on a cheap, cloneable [`MirrorHandle`] (a DashMap insert of a
//! small value — no serialization, no I/O, no awaits). A spawned flush task
//! drains the dirty set every 250ms, serializes to JSON, and writes one
//! pipelined batch: `vw:quote:{instrument}` and `vw:match:{match_id}` with a
//! 1-hour TTL.
//!
//! ## Outage semantics
//!
//! A Redis outage must never stall ingestion. On a failed flush the batch is
//! **dropped** (state is re-mirrored naturally by the next tick touching it),
//! the error counter increments (feeds the future `vw_redis_write_errors_total`
//! metric), and the outage transition is logged exactly once. On the first
//! successful flush after an outage, recovery is logged once and normal
//! operation resumes — no queue growth, no backpressure, ever.
//!
//! The sink is abstracted behind [`QuoteSink`] so the full behavior is tested
//! without a Redis server; [`RedisSink`] is the production implementation.

use crate::book::LatestQuote;
use crate::matchview::MatchView;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::Serialize;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use vw_core::InstrumentId;

/// Spec §7.1: flush dirty state every 250ms.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
/// Spec §7.1: mirrored keys expire after one hour.
pub const DEFAULT_TTL_SECS: u64 = 3600;

/// One key/value ready to be written to the mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorEntry {
    pub key: String,
    pub json: String,
    pub ttl_secs: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("mirror sink error: {0}")]
pub struct SinkError(pub String);

/// Where mirrored entries go. One `write` call is one atomic-ish batch
/// (pipelined for Redis); implementations must be cancel-safe enough that a
/// failed batch can simply be dropped.
#[async_trait]
pub trait QuoteSink: Send + Sync {
    async fn write(&self, entries: Vec<MirrorEntry>) -> Result<(), SinkError>;
}

#[async_trait]
impl<S: QuoteSink + ?Sized> QuoteSink for Arc<S> {
    async fn write(&self, entries: Vec<MirrorEntry>) -> Result<(), SinkError> {
        (**self).write(entries).await
    }
}

/// Pending dirty value, cloned cheaply on the hot path and serialized lazily
/// in the flush task. Keyed by the final Redis key, so repeated updates to the
/// same instrument/match coalesce to the latest value between flushes.
#[derive(Debug, Clone)]
enum Pending {
    Quote {
        instrument: InstrumentId,
        quote: LatestQuote,
    },
    Match(MatchView),
}

#[derive(Serialize)]
struct QuoteRecord<'a> {
    instrument: &'a InstrumentId,
    #[serde(flatten)]
    quote: &'a LatestQuote,
}

impl Pending {
    fn to_json(&self) -> String {
        match self {
            Pending::Quote { instrument, quote } => {
                serde_json::to_string(&QuoteRecord { instrument, quote })
            }
            Pending::Match(view) => serde_json::to_string(view),
        }
        .expect("mirror payloads are always serializable")
    }
}

#[derive(Debug)]
struct MirrorInner {
    dirty: DashMap<String, Pending>,
    ttl_secs: u64,
    write_errors: AtomicU64,
    in_outage: AtomicBool,
    shutdown: AtomicBool,
}

/// Cheap, cloneable handle used on the hot path to mark state dirty, plus the
/// flush machinery ([`MirrorHandle::spawn`] / [`MirrorHandle::flush_once`]).
#[derive(Debug, Clone)]
pub struct MirrorHandle {
    inner: Arc<MirrorInner>,
}

impl Default for MirrorHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl MirrorHandle {
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_TTL_SECS)
    }

    pub fn with_ttl(ttl_secs: u64) -> Self {
        Self {
            inner: Arc::new(MirrorInner {
                dirty: DashMap::new(),
                ttl_secs,
                write_errors: AtomicU64::new(0),
                in_outage: AtomicBool::new(false),
                shutdown: AtomicBool::new(false),
            }),
        }
    }

    /// Hot path: record the latest quote for `instrument` as dirty.
    /// Synchronous; a single DashMap shard insert of a `Copy` payload.
    pub fn mark_quote(&self, instrument: &InstrumentId, quote: &LatestQuote) {
        self.inner.dirty.insert(
            format!("vw:quote:{instrument}"),
            Pending::Quote {
                instrument: instrument.clone(),
                quote: *quote,
            },
        );
    }

    /// Hot path: record an updated match view as dirty.
    pub fn mark_match(&self, view: &MatchView) {
        self.inner.dirty.insert(
            format!("vw:match:{}", view.match_id),
            Pending::Match(view.clone()),
        );
    }

    /// Number of entries waiting for the next flush.
    pub fn pending_len(&self) -> usize {
        self.inner.dirty.len()
    }

    /// Failed flush batches since startup (future `vw_redis_write_errors_total`).
    pub fn write_error_count(&self) -> u64 {
        self.inner.write_errors.load(Ordering::Relaxed)
    }

    /// Whether the mirror is currently in an outage (last flush failed).
    pub fn in_outage(&self) -> bool {
        self.inner.in_outage.load(Ordering::Relaxed)
    }

    /// Ask the spawned flush task to exit after one final flush.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Relaxed);
    }

    /// Drain the dirty set, serialize, and write one pipelined batch to `sink`.
    /// On failure the batch is dropped and the outage state updated — this
    /// never returns an error and never blocks the hot path (markers keep
    /// landing in the dirty map concurrently).
    pub async fn flush_once<S: QuoteSink + ?Sized>(&self, sink: &S) {
        let keys: Vec<String> = self.inner.dirty.iter().map(|e| e.key().clone()).collect();
        if keys.is_empty() {
            return;
        }
        let mut entries = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((key, pending)) = self.inner.dirty.remove(&key) {
                entries.push(MirrorEntry {
                    json: pending.to_json(),
                    key,
                    ttl_secs: self.inner.ttl_secs,
                });
            }
        }
        if entries.is_empty() {
            return;
        }
        let batch_len = entries.len();
        match sink.write(entries).await {
            Ok(()) => {
                if self.inner.in_outage.swap(false, Ordering::Relaxed) {
                    tracing::info!("redis mirror recovered; resuming writes");
                }
            }
            Err(e) => {
                self.inner.write_errors.fetch_add(1, Ordering::Relaxed);
                if !self.inner.in_outage.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        error = %e,
                        dropped = batch_len,
                        "redis mirror outage; dropping mirror writes until recovery"
                    );
                }
            }
        }
    }

    /// Spawn the write-behind flush task (every `flush_interval`, spec default
    /// [`DEFAULT_FLUSH_INTERVAL`]). The task runs until [`MirrorHandle::shutdown`]
    /// is called (one final flush) or the returned handle is aborted.
    ///
    /// Must be called from within a tokio runtime.
    pub fn spawn<S: QuoteSink + 'static>(
        &self,
        sink: S,
        flush_interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let handle = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(flush_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                handle.flush_once(&sink).await;
                if handle.inner.shutdown.load(Ordering::Relaxed) {
                    handle.flush_once(&sink).await;
                    return;
                }
            }
        })
    }
}

/// Production sink: pipelined `SET key json EX ttl` via a reconnecting
/// [`redis::aio::ConnectionManager`].
#[derive(Clone)]
pub struct RedisSink {
    conn: redis::aio::ConnectionManager,
}

impl fmt::Debug for RedisSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisSink").finish_non_exhaustive()
    }
}

impl RedisSink {
    /// Connect to Redis at `url` (e.g. `RedisConfig::url`). Fails only if the
    /// initial connection cannot be established; afterwards the connection
    /// manager reconnects on its own and per-batch failures surface as
    /// [`SinkError`]s, which the mirror tolerates.
    pub async fn connect(url: &str) -> Result<Self, SinkError> {
        let client = redis::Client::open(url).map_err(|e| SinkError(e.to_string()))?;
        let conn = client
            .get_connection_manager()
            .await
            .map_err(|e| SinkError(e.to_string()))?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl QuoteSink for RedisSink {
    async fn write(&self, entries: Vec<MirrorEntry>) -> Result<(), SinkError> {
        let mut pipe = redis::pipe();
        for entry in &entries {
            pipe.cmd("SET")
                .arg(&entry.key)
                .arg(&entry.json)
                .arg("EX")
                .arg(entry.ttl_secs)
                .ignore();
        }
        let mut conn = self.conn.clone();
        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| SinkError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;
    use std::sync::Mutex;

    /// Records every batch; can be flipped into failure mode.
    #[derive(Debug, Default)]
    struct TestSink {
        batches: Mutex<Vec<Vec<MirrorEntry>>>,
        fail: AtomicBool,
    }

    impl TestSink {
        fn set_failing(&self, failing: bool) {
            self.fail.store(failing, Ordering::Relaxed);
        }
        fn batches(&self) -> Vec<Vec<MirrorEntry>> {
            self.batches.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl QuoteSink for TestSink {
        async fn write(&self, entries: Vec<MirrorEntry>) -> Result<(), SinkError> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(SinkError("simulated redis outage".into()));
            }
            self.batches.lock().unwrap().push(entries);
            Ok(())
        }
    }

    fn quote(seq: u64) -> LatestQuote {
        LatestQuote {
            yes_bid: Some(dec!(0.40)),
            yes_ask: Some(dec!(0.44)),
            last_price: Some(dec!(0.41)),
            venue_ts: None,
            recv_ts: Utc::now(),
            seq,
            updated_at: Utc::now(),
        }
    }

    fn iid(s: &str) -> InstrumentId {
        InstrumentId(s.to_string())
    }

    #[tokio::test]
    async fn flush_writes_keys_json_and_ttl_then_clears_dirty() {
        let mirror = MirrorHandle::new();
        let sink = TestSink::default();

        mirror.mark_quote(&iid("kalshi:FED"), &quote(1));
        mirror.mark_match(&MatchView {
            match_id: "fed-cut".into(),
            legs: vec![],
            best_yes_bid: Some(dec!(0.42)),
            best_yes_ask: Some(dec!(0.44)),
        });
        assert_eq!(mirror.pending_len(), 2);

        mirror.flush_once(&sink).await;
        assert_eq!(mirror.pending_len(), 0);

        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        let mut batch = batches[0].clone();
        batch.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(batch.len(), 2);

        assert_eq!(batch[0].key, "vw:match:fed-cut");
        assert_eq!(batch[0].ttl_secs, DEFAULT_TTL_SECS);
        let m: serde_json::Value = serde_json::from_str(&batch[0].json).unwrap();
        assert_eq!(m["match_id"], "fed-cut");
        assert_eq!(m["best_yes_bid"], "0.42");

        assert_eq!(batch[1].key, "vw:quote:kalshi:FED");
        assert_eq!(batch[1].ttl_secs, DEFAULT_TTL_SECS);
        let q: serde_json::Value = serde_json::from_str(&batch[1].json).unwrap();
        assert_eq!(q["instrument"], "kalshi:FED");
        assert_eq!(q["yes_bid"], "0.40");
        assert_eq!(q["seq"], 1);

        // nothing dirty → no extra batch
        mirror.flush_once(&sink).await;
        assert_eq!(sink.batches().len(), 1);
    }

    #[tokio::test]
    async fn updates_between_flushes_coalesce_to_latest() {
        let mirror = MirrorHandle::new();
        let sink = TestSink::default();

        mirror.mark_quote(&iid("kalshi:FED"), &quote(1));
        mirror.mark_quote(&iid("kalshi:FED"), &quote(2));
        mirror.mark_quote(&iid("kalshi:FED"), &quote(3));
        assert_eq!(mirror.pending_len(), 1);

        mirror.flush_once(&sink).await;
        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        let q: serde_json::Value = serde_json::from_str(&batches[0][0].json).unwrap();
        assert_eq!(q["seq"], 3);
    }

    #[tokio::test]
    async fn outage_drops_writes_counts_errors_and_recovers() {
        let mirror = MirrorHandle::new();
        let sink = TestSink::default();
        sink.set_failing(true);

        mirror.mark_quote(&iid("kalshi:A"), &quote(1));
        mirror.flush_once(&sink).await;
        assert_eq!(mirror.write_error_count(), 1);
        assert!(mirror.in_outage());
        // batch was dropped, not retried
        assert_eq!(mirror.pending_len(), 0);
        assert!(sink.batches().is_empty());

        // still failing: counter keeps counting, no queue growth
        mirror.mark_quote(&iid("kalshi:A"), &quote(2));
        mirror.flush_once(&sink).await;
        assert_eq!(mirror.write_error_count(), 2);
        assert_eq!(mirror.pending_len(), 0);

        // redis comes back: next dirty state flows through, outage clears
        sink.set_failing(false);
        mirror.mark_quote(&iid("kalshi:A"), &quote(3));
        mirror.flush_once(&sink).await;
        assert!(!mirror.in_outage());
        assert_eq!(mirror.write_error_count(), 2);
        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        let q: serde_json::Value = serde_json::from_str(&batches[0][0].json).unwrap();
        assert_eq!(q["seq"], 3);
    }

    #[tokio::test(start_paused = true)]
    async fn spawned_task_flushes_on_interval_and_shuts_down() {
        let mirror = MirrorHandle::new();
        let sink = Arc::new(TestSink::default());
        let task = mirror.spawn(Arc::clone(&sink), DEFAULT_FLUSH_INTERVAL);

        mirror.mark_quote(&iid("kalshi:A"), &quote(1));
        // paused clock: advancing past one interval triggers exactly one flush
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(sink.batches().len(), 1);
        assert_eq!(mirror.pending_len(), 0);

        mirror.mark_quote(&iid("kalshi:A"), &quote(2));
        mirror.shutdown();
        tokio::time::sleep(Duration::from_millis(300)).await;
        task.await.unwrap();
        // the final flush picked up the last marker
        assert_eq!(sink.batches().len(), 2);
    }

    /// Live smoke test against a real Redis; requires `redis://localhost:6379`.
    /// Run with `cargo test -p vw-state -- --ignored live_redis`.
    #[tokio::test]
    #[ignore = "requires a running Redis at localhost:6379"]
    async fn live_redis_roundtrip() {
        let sink = RedisSink::connect("redis://localhost:6379")
            .await
            .expect("redis reachable");
        let mirror = MirrorHandle::new();
        mirror.mark_quote(&iid("kalshi:LIVE-TEST"), &quote(1));
        mirror.flush_once(&sink).await;
        assert!(!mirror.in_outage());
        assert_eq!(mirror.write_error_count(), 0);
    }
}
