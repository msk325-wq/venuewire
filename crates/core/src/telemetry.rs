//! Tracing initialization shared by all binaries.

use tracing_subscriber::{fmt, EnvFilter};

/// Initialize the global tracing subscriber.
///
/// Respects `RUST_LOG` (default `info`). `json = true` emits one JSON object
/// per line for log shippers; `false` gives human-readable output for dev.
pub fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt().with_env_filter(filter).with_target(true);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}
