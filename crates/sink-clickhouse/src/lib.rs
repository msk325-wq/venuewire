//! Batching ClickHouse writer (M5, spec §7.3).
//!
//! Consumes [`SinkEvent`]s (ticks and divergences) from a bounded mpsc,
//! encodes them to JSONEachRow lines as they arrive, and flushes per table
//! every `flush_interval` (default 500ms) **or** every `max_batch_rows`
//! (default 5_000), whichever comes first.
//!
//! ## Outage behavior
//!
//! A ClickHouse outage must never stall ingest. The run loop always keeps
//! draining its channel; failed batches are parked in a FIFO retry queue and
//! retried in order on the next flush trigger. The total buffered row count
//! (retry queue + accumulating buffers) is capped at `max_buffer_rows`
//! (default 100_000); beyond that the **oldest batch** is dropped and counted
//! in `vw_clickhouse_rows_dropped_total`. See decisions draft D-M5-3.
//!
//! ## Wiring (daemon)
//!
//! ```ignore
//! let (sink, tx) = ClickHouseSink::new(SinkOptions::from_config(&cfg.clickhouse))?;
//! sink.ensure_schema().await?;
//! let metrics = sink.metrics();
//! tokio::spawn(sink.run());
//! // forward ticks/divergences: tx.send(SinkEvent::Tick(tick)).await
//! ```

pub mod metrics;
pub mod rows;
pub mod schema;
pub mod transport;

pub use metrics::SinkMetrics;
pub use schema::{DIVERGENCES_DDL, TICKS_DDL};
pub use transport::{HttpTransport, Transport, TransportError};

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use vw_core::config::ClickHouseConfig;
use vw_core::{DivergenceEvent, Tick};

/// Everything the sink persists.
#[derive(Debug, Clone)]
pub enum SinkEvent {
    Tick(Tick),
    Divergence(DivergenceEvent),
}

/// Sink tuning. Defaults match spec §7.3.
#[derive(Clone)]
pub struct SinkOptions {
    /// ClickHouse HTTP endpoint, e.g. `http://localhost:8123`.
    pub url: String,
    pub database: String,
    /// Per-request timeout for the HTTP transport.
    pub request_timeout: Duration,
    /// Flush cadence (spec: 500ms).
    pub flush_interval: Duration,
    /// Flush as soon as one table's buffer reaches this many rows (spec: 5_000).
    pub max_batch_rows: usize,
    /// Cap on total buffered rows (retry queue + accumulating buffers) during
    /// an outage; beyond it the oldest batch is dropped.
    pub max_buffer_rows: usize,
    /// Capacity of the mpsc handed to the daemon.
    pub channel_capacity: usize,
    /// Register the sink's metrics (including
    /// `vw_clickhouse_batch_flush_seconds`) on this registry.
    pub registry: Option<prometheus::Registry>,
}

impl Default for SinkOptions {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".into(),
            database: "default".into(),
            request_timeout: Duration::from_secs(5),
            flush_interval: Duration::from_millis(500),
            max_batch_rows: 5_000,
            max_buffer_rows: 100_000,
            channel_capacity: 10_000,
            registry: None,
        }
    }
}

impl SinkOptions {
    /// Defaults with the URL taken from the shared config.
    pub fn from_config(cfg: &ClickHouseConfig) -> Self {
        Self {
            url: cfg.url.clone(),
            ..Self::default()
        }
    }
}

impl fmt::Debug for SinkOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkOptions")
            .field("url", &self.url)
            .field("database", &self.database)
            .field("request_timeout", &self.request_timeout)
            .field("flush_interval", &self.flush_interval)
            .field("max_batch_rows", &self.max_batch_rows)
            .field("max_buffer_rows", &self.max_buffer_rows)
            .field("channel_capacity", &self.channel_capacity)
            .field("registry", &self.registry.as_ref().map(|_| "Registry"))
            .finish()
    }
}

const TABLES: [&str; 2] = ["ticks", "divergences"];
const TICKS: usize = 0;
const DIVERGENCES: usize = 1;

/// A sealed batch awaiting insert: pre-encoded JSONEachRow lines for one table.
#[derive(Debug)]
struct Batch {
    table: usize,
    rows: Vec<String>,
}

/// The batching writer. Construct, [`ensure_schema`](Self::ensure_schema),
/// then spawn [`run`](Self::run); feed events through the returned sender.
#[derive(Debug)]
pub struct ClickHouseSink {
    transport: Arc<dyn Transport>,
    rx: mpsc::Receiver<SinkEvent>,
    flush_interval: Duration,
    max_batch_rows: usize,
    max_buffer_rows: usize,
    metrics: Arc<SinkMetrics>,
    /// Accumulating (unsealed) rows, one buffer per table.
    current: [Vec<String>; 2],
    /// Sealed batches awaiting (re-)insert, oldest first.
    pending: VecDeque<Batch>,
    pending_rows: usize,
}

impl ClickHouseSink {
    /// Production constructor: HTTP transport from `options.url`.
    pub fn new(options: SinkOptions) -> Result<(Self, mpsc::Sender<SinkEvent>), TransportError> {
        let transport =
            HttpTransport::new(&options.url, &options.database, options.request_timeout)?;
        Ok(Self::with_transport(options, Arc::new(transport)))
    }

    /// Constructor with an injected transport (tests, alternative protocols).
    pub fn with_transport(
        options: SinkOptions,
        transport: Arc<dyn Transport>,
    ) -> (Self, mpsc::Sender<SinkEvent>) {
        let metrics = Arc::new(SinkMetrics::new());
        if let Some(registry) = &options.registry {
            if let Err(e) = metrics.register(registry) {
                tracing::warn!(error = %e, "failed to register clickhouse sink metrics");
            }
        }
        let (tx, rx) = mpsc::channel(options.channel_capacity.max(1));
        let sink = Self {
            transport,
            rx,
            flush_interval: options.flush_interval,
            max_batch_rows: options.max_batch_rows.max(1),
            max_buffer_rows: options.max_buffer_rows.max(1),
            metrics,
            current: [Vec::new(), Vec::new()],
            pending: VecDeque::new(),
            pending_rows: 0,
        };
        (sink, tx)
    }

    /// Counters and the flush-duration histogram (clone before `run` consumes
    /// the sink).
    pub fn metrics(&self) -> Arc<SinkMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Idempotently create the `ticks` and `divergences` tables (§7.3).
    pub async fn ensure_schema(&self) -> Result<(), TransportError> {
        self.transport.execute(TICKS_DDL, String::new()).await?;
        self.transport
            .execute(DIVERGENCES_DDL, String::new())
            .await?;
        Ok(())
    }

    /// Consume events until the channel closes, then attempt a final flush.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Consume the immediate first tick so the timer branch first fires
        // one full `flush_interval` from now.
        interval.tick().await;
        loop {
            tokio::select! {
                event = self.rx.recv() => match event {
                    Some(event) => {
                        if self.ingest(event) {
                            self.flush().await;
                        }
                    }
                    None => break,
                },
                _ = interval.tick() => {
                    self.seal_all();
                    self.flush().await;
                }
            }
        }
        self.seal_all();
        self.flush().await;
        if self.pending_rows > 0 {
            tracing::warn!(
                rows = self.pending_rows,
                "clickhouse sink shutting down with unflushed rows"
            );
        }
    }

    /// Encode and buffer one event. Returns true if a buffer hit
    /// `max_batch_rows` and a flush should run now.
    fn ingest(&mut self, event: SinkEvent) -> bool {
        let (table, row) = match event {
            SinkEvent::Tick(t) => (TICKS, rows::tick_row(&t)),
            SinkEvent::Divergence(d) => (DIVERGENCES, rows::divergence_row(&d)),
        };
        self.current[table].push(row);
        self.enforce_cap();
        if self.current[table].len() >= self.max_batch_rows {
            self.seal(table);
            true
        } else {
            false
        }
    }

    fn buffered_rows(&self) -> usize {
        self.pending_rows + self.current[TICKS].len() + self.current[DIVERGENCES].len()
    }

    /// Move one table's accumulating buffer into the retry queue.
    fn seal(&mut self, table: usize) {
        if self.current[table].is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.current[table]);
        self.pending_rows += rows.len();
        self.pending.push_back(Batch { table, rows });
    }

    fn seal_all(&mut self) {
        self.seal(TICKS);
        self.seal(DIVERGENCES);
    }

    /// Drop-oldest-batch until the buffer fits (D-M5-3). Only triggers while
    /// flushes are failing.
    fn enforce_cap(&mut self) {
        while self.buffered_rows() > self.max_buffer_rows {
            let dropped = if let Some(batch) = self.pending.pop_front() {
                self.pending_rows -= batch.rows.len();
                (TABLES[batch.table], batch.rows.len())
            } else if !self.current[TICKS].is_empty() {
                (
                    TABLES[TICKS],
                    std::mem::take(&mut self.current[TICKS]).len(),
                )
            } else {
                (
                    TABLES[DIVERGENCES],
                    std::mem::take(&mut self.current[DIVERGENCES]).len(),
                )
            };
            self.metrics.rows_dropped.inc_by(dropped.1 as u64);
            tracing::warn!(
                table = dropped.0,
                rows = dropped.1,
                buffered = self.buffered_rows(),
                "clickhouse buffer over cap; dropped oldest batch"
            );
        }
    }

    /// Insert pending batches oldest-first; stop at the first failure (the
    /// failed batch stays queued for the next trigger).
    async fn flush(&mut self) {
        while let Some(batch) = self.pending.front() {
            let query = format!("INSERT INTO {} FORMAT JSONEachRow", TABLES[batch.table]);
            let body = batch.rows.join("\n");
            let started = Instant::now();
            match self.transport.execute(&query, body).await {
                Ok(()) => {
                    self.metrics
                        .flush_seconds
                        .observe(started.elapsed().as_secs_f64());
                    let batch = self.pending.pop_front().expect("front exists");
                    self.pending_rows -= batch.rows.len();
                    self.metrics.rows_written.inc_by(batch.rows.len() as u64);
                }
                Err(e) => {
                    self.metrics.flush_errors.inc();
                    tracing::warn!(
                        table = TABLES[batch.table],
                        rows = batch.rows.len(),
                        buffered = self.buffered_rows(),
                        error = %e,
                        "clickhouse batch flush failed; batch retained for retry"
                    );
                    break;
                }
            }
        }
    }
}
