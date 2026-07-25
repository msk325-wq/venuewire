//! REST snapshot endpoints (spec §7.4). Every handler takes a point-in-time
//! snapshot from `PipelineState` synchronously — no lock is ever held across
//! an await point.

use crate::AppState;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use rust_decimal::Decimal;
use serde::Serialize;
use vw_core::InstrumentId;
use vw_state::LatestQuote;

#[derive(Debug, Serialize)]
pub(crate) struct Health {
    status: &'static str,
    instruments: usize,
    matches: usize,
    ticks_accepted: u64,
    ticks_rejected: u64,
    ws_clients: i64,
}

pub(crate) async fn health(State(app): State<AppState>) -> Json<Health> {
    let (accepted, rejected) = app.pipeline.tick_counts();
    Json(Health {
        status: "ok",
        instruments: app.pipeline.instrument_count(),
        matches: app.pipeline.match_count(),
        ticks_accepted: accepted,
        ticks_rejected: rejected,
        ws_clients: app.metrics.ws_clients(),
    })
}

/// One instrument's latest quote plus its derived mid.
#[derive(Debug, Serialize)]
pub(crate) struct QuoteEntry {
    instrument: InstrumentId,
    #[serde(flatten)]
    quote: LatestQuote,
    mid: Option<Decimal>,
}

impl QuoteEntry {
    fn new(instrument: InstrumentId, quote: LatestQuote) -> Self {
        let mid = quote.yes_mid();
        Self {
            instrument,
            quote,
            mid,
        }
    }
}

pub(crate) async fn instruments(State(app): State<AppState>) -> Json<Vec<QuoteEntry>> {
    let mut quotes = app.pipeline.all_quotes();
    quotes.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
    Json(
        quotes
            .into_iter()
            .map(|(id, q)| QuoteEntry::new(id, q))
            .collect(),
    )
}

fn not_found(what: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": format!("unknown {what}") })),
    )
        .into_response()
}

pub(crate) async fn quote(State(app): State<AppState>, Path(instrument): Path<String>) -> Response {
    let id = InstrumentId(instrument);
    match app.pipeline.quote(&id) {
        Some(q) => Json(QuoteEntry::new(id, q)).into_response(),
        None => not_found("instrument"),
    }
}

pub(crate) async fn matches(State(app): State<AppState>) -> Response {
    let mut views = app.pipeline.all_match_views();
    views.sort_by(|a, b| a.match_id.cmp(&b.match_id));
    Json(views).into_response()
}

pub(crate) async fn match_view(State(app): State<AppState>, Path(id): Path<String>) -> Response {
    match app.pipeline.match_view(&id) {
        Some(view) => Json(view).into_response(),
        None => not_found("match"),
    }
}

pub(crate) async fn metrics(State(app): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        app.metrics.encode(),
    )
        .into_response()
}
