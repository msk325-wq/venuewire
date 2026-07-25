//! Static bearer-token middleware. When a token is configured, every route
//! except `/health` and `/metrics` (scrapers and probes stay unauthenticated)
//! requires `Authorization: Bearer <token>`, compared in constant time.

use crate::AppState;
use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use subtle::ConstantTimeEq;

/// Routes exempt from auth even when a token is configured.
fn exempt(path: &str) -> bool {
    matches!(path, "/health" | "/metrics")
}

pub(crate) async fn require_bearer(
    State(app): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    if app.auth_token.is_empty() || exempt(req.uri().path()) {
        return next.run(req).await;
    }
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    // `ct_eq` on byte slices short-circuits only on length (not secret) and
    // compares contents in constant time.
    let ok = presented
        .map(|t| bool::from(t.as_bytes().ct_eq(app.auth_token.as_bytes())))
        .unwrap_or(false);
    if ok {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "missing or invalid bearer token" })),
        )
            .into_response()
    }
}
