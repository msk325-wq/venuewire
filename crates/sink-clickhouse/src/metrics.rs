//! Sink counters and the flush-duration histogram.
//!
//! Metrics are always live (plain `prometheus` primitives readable via the
//! getter methods, so tests and the daemon can bridge them anywhere) and are
//! additionally registered on a caller-provided `prometheus::Registry` when
//! [`crate::SinkOptions::registry`] is set — that is how the M6 daemon gets
//! `vw_clickhouse_batch_flush_seconds` onto the server's `/metrics` page.

use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};
use std::fmt;

pub struct SinkMetrics {
    pub(crate) rows_written: IntCounter,
    pub(crate) rows_dropped: IntCounter,
    pub(crate) flush_errors: IntCounter,
    pub(crate) flush_seconds: Histogram,
}

impl SinkMetrics {
    pub(crate) fn new() -> Self {
        Self {
            rows_written: IntCounter::new(
                "vw_clickhouse_rows_written_total",
                "Rows successfully inserted into ClickHouse",
            )
            .expect("valid metric"),
            rows_dropped: IntCounter::new(
                "vw_clickhouse_rows_dropped_total",
                "Rows dropped because the outage buffer overflowed",
            )
            .expect("valid metric"),
            flush_errors: IntCounter::new(
                "vw_clickhouse_flush_errors_total",
                "Failed batch flush attempts",
            )
            .expect("valid metric"),
            flush_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "vw_clickhouse_batch_flush_seconds",
                    "Duration of successful ClickHouse batch flushes",
                )
                .buckets(prometheus::exponential_buckets(0.001, 2.0, 14).expect("valid buckets")),
            )
            .expect("valid metric"),
        }
    }

    /// Register all sink metrics on `registry`.
    pub(crate) fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.rows_written.clone()))?;
        registry.register(Box::new(self.rows_dropped.clone()))?;
        registry.register(Box::new(self.flush_errors.clone()))?;
        registry.register(Box::new(self.flush_seconds.clone()))?;
        Ok(())
    }

    /// Rows successfully inserted since startup.
    pub fn rows_written(&self) -> u64 {
        self.rows_written.get()
    }

    /// Rows dropped (oldest-batch policy) since startup.
    pub fn rows_dropped(&self) -> u64 {
        self.rows_dropped.get()
    }

    /// Failed flush attempts since startup.
    pub fn flush_errors(&self) -> u64 {
        self.flush_errors.get()
    }

    /// Number of successful flushes (histogram sample count).
    pub fn flush_count(&self) -> u64 {
        self.flush_seconds.get_sample_count()
    }
}

impl fmt::Debug for SinkMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkMetrics")
            .field("rows_written", &self.rows_written())
            .field("rows_dropped", &self.rows_dropped())
            .field("flush_errors", &self.flush_errors())
            .field("flush_count", &self.flush_count())
            .finish()
    }
}
