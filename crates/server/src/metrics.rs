//! Server-owned Prometheus registry and the M5 metrics (spec §9):
//! `vw_ws_clients` and `vw_ws_lagged_total`. The registry is public so the
//! M6 daemon can register the remaining pipeline metrics (and the ClickHouse
//! sink's) on the same `/metrics` page.

use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};
use std::fmt;

pub struct ServerMetrics {
    registry: Registry,
    pub(crate) ws_clients: IntGauge,
    pub(crate) ws_lagged_total: IntCounter,
}

impl ServerMetrics {
    pub(crate) fn new() -> Self {
        let registry = Registry::new();
        let ws_clients = IntGauge::new("vw_ws_clients", "Currently connected WebSocket clients")
            .expect("valid metric");
        let ws_lagged_total = IntCounter::new(
            "vw_ws_lagged_total",
            "Broadcast messages missed by lagging WebSocket clients",
        )
        .expect("valid metric");
        registry
            .register(Box::new(ws_clients.clone()))
            .expect("fresh registry");
        registry
            .register(Box::new(ws_lagged_total.clone()))
            .expect("fresh registry");
        Self {
            registry,
            ws_clients,
            ws_lagged_total,
        }
    }

    /// The registry backing `GET /metrics`. Register more collectors here
    /// (daemon/sink metrics in M6).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Current `vw_ws_clients` value.
    pub fn ws_clients(&self) -> i64 {
        self.ws_clients.get()
    }

    /// Current `vw_ws_lagged_total` value.
    pub fn ws_lagged_total(&self) -> u64 {
        self.ws_lagged_total.get()
    }

    /// Prometheus text exposition of everything in the registry.
    pub(crate) fn encode(&self) -> String {
        let mut buf = Vec::new();
        if let Err(e) = TextEncoder::new().encode(&self.registry.gather(), &mut buf) {
            tracing::warn!(error = %e, "failed to encode metrics");
        }
        String::from_utf8(buf).unwrap_or_default()
    }
}

impl fmt::Debug for ServerMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerMetrics")
            .field("ws_clients", &self.ws_clients())
            .field("ws_lagged_total", &self.ws_lagged_total())
            .finish()
    }
}
