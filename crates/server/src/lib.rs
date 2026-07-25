//! axum serving layer (M5, spec §7.4): REST snapshots over
//! [`PipelineState`], WebSocket fan-out with lagged-client handling, a
//! server-owned Prometheus registry, and optional static bearer auth.
//!
//! ## Endpoints
//!
//! | Route | Description |
//! |---|---|
//! | `GET /health` | liveness + pipeline counters (never authed) |
//! | `GET /instruments` | latest quote per instrument |
//! | `GET /quotes/{instrument}` | latest quote for one instrument |
//! | `GET /matches` | live view of every matched market |
//! | `GET /matches/{id}/view` | live view of one match |
//! | `GET /metrics` | Prometheus text format (never authed) |
//! | `GET /ws` | WebSocket stream, see [`ws`] module docs |
//!
//! ## Wiring (daemon)
//!
//! ```ignore
//! let server = Server::new(state, ServerOptions::from_config(&cfg.server));
//! let publish = server.publish.clone();       // feed ticks/divergences here
//! let registry = server.metrics.registry();   // register M6 metrics here
//! let listener = tokio::net::TcpListener::bind(&cfg.server.bind).await?;
//! axum::serve(listener, server.router).await?;
//! ```

mod auth;
pub mod events;
pub mod metrics;
mod rest;
pub mod ws;

pub use events::{PublishHandle, StreamEvent};
pub use metrics::ServerMetrics;

use axum::routing::get;
use axum::Router;
use std::sync::Arc;
use tokio::sync::broadcast;
use vw_core::config::ServerConfig;
use vw_state::PipelineState;

/// Server construction options.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// Static bearer token; empty disables auth (local-dev default).
    pub auth_token: String,
    /// Capacity of the WS fan-out broadcast channel: how far a slow client
    /// may fall behind before it starts missing events.
    pub broadcast_capacity: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            auth_token: String::new(),
            broadcast_capacity: 1024,
        }
    }
}

impl ServerOptions {
    /// Defaults with the auth token taken from the shared config.
    pub fn from_config(cfg: &ServerConfig) -> Self {
        Self {
            auth_token: cfg.auth_token.clone(),
            ..Self::default()
        }
    }
}

/// Shared handler state.
#[derive(Debug, Clone)]
pub(crate) struct AppState {
    pub(crate) pipeline: Arc<PipelineState>,
    pub(crate) events: broadcast::Sender<StreamEvent>,
    pub(crate) metrics: Arc<ServerMetrics>,
    pub(crate) auth_token: Arc<str>,
}

/// The built server: hand `router` to `axum::serve`, publish events through
/// `publish`, and register additional metrics on `metrics.registry()`.
#[derive(Debug)]
pub struct Server {
    pub router: Router,
    pub publish: PublishHandle,
    pub metrics: Arc<ServerMetrics>,
}

impl Server {
    pub fn new(pipeline: Arc<PipelineState>, options: ServerOptions) -> Self {
        let (events, _) = broadcast::channel(options.broadcast_capacity.max(1));
        let metrics = Arc::new(ServerMetrics::new());
        let app = AppState {
            pipeline,
            events: events.clone(),
            metrics: Arc::clone(&metrics),
            auth_token: options.auth_token.into(),
        };
        let router = Router::new()
            .route("/health", get(rest::health))
            .route("/instruments", get(rest::instruments))
            .route("/quotes/{instrument}", get(rest::quote))
            .route("/matches", get(rest::matches))
            .route("/matches/{id}/view", get(rest::match_view))
            .route("/metrics", get(rest::metrics))
            .route("/ws", get(ws::ws_handler))
            .layer(axum::middleware::from_fn_with_state(
                app.clone(),
                auth::require_bearer,
            ))
            .with_state(app);
        Self {
            router,
            publish: PublishHandle::new(events),
            metrics,
        }
    }
}
