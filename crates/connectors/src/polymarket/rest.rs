//! Polymarket Gamma REST discovery client (unauthenticated; see
//! docs/research/polymarket-api.md §1.1).
//!
//! Discovery is tag-based: configured tag *slugs* resolve to numeric tag ids
//! via `GET /tags/slug/{slug}`, then `GET /markets?tag_id=…` pages active
//! markets by 24h volume (offset pagination). The CLOB REST base
//! (`[polymarket].clob_url`) is reserved for `/book` snapshot reconciliation;
//! v1 seeds prices from Gamma snapshots + the WSS book pushed on subscribe.

use anyhow::Context;
use serde_json::Value;
use std::time::Duration;

/// Page size for `GET /markets` (offset pagination).
const PAGE_LIMIT: u32 = 100;

/// Safety cap on pagination depth per tag, to stay polite even if a tag
/// matches a huge universe.
const MAX_PAGES: u32 = 10;

#[derive(Debug, Clone)]
pub struct PolymarketGamma {
    http: reqwest::Client,
    base: String,
}

impl PolymarketGamma {
    pub fn new(gamma_url: &str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("venuewire/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(10))
            .build()
            .context("building Polymarket Gamma REST client")?;
        Ok(PolymarketGamma {
            http,
            base: gamma_url.trim_end_matches('/').to_string(),
        })
    }

    /// Resolve a tag slug to its numeric id (`GET /tags/slug/{slug}`).
    /// Returns `None` (with a warning) for unknown slugs so one typo doesn't
    /// take down discovery for the other tags.
    pub async fn resolve_tag(&self, slug: &str) -> anyhow::Result<Option<String>> {
        let url = format!("{}/tags/slug/{slug}", self.base);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET /tags/slug/{slug}"))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            tracing::warn!(slug, "polymarket tag slug not found; skipping");
            return Ok(None);
        }
        let body: Value = resp
            .error_for_status()
            .with_context(|| format!("GET /tags/slug/{slug}"))?
            .json()
            .await
            .context("decoding /tags response")?;
        Ok(body.get("id").and_then(Value::as_str).map(str::to_string))
    }

    /// Active, order-book-enabled markets for one tag id, highest 24h volume
    /// first, up to `max`. Returns raw Gamma market objects verbatim
    /// (normalization happens in [`super::normalize`], off the recorded frame).
    pub async fn active_markets(&self, tag_id: &str, max: usize) -> anyhow::Result<Vec<Value>> {
        let url = format!("{}/markets", self.base);
        let mut markets: Vec<Value> = Vec::new();
        for page in 0..MAX_PAGES {
            let offset = page * PAGE_LIMIT;
            let query: Vec<(&str, String)> = vec![
                ("tag_id", tag_id.to_string()),
                ("active", "true".into()),
                ("closed", "false".into()),
                ("order", "volume24hr".into()),
                ("ascending", "false".into()),
                ("limit", PAGE_LIMIT.to_string()),
                ("offset", offset.to_string()),
            ];
            let body: Value = self
                .http
                .get(&url)
                .query(&query)
                .send()
                .await
                .with_context(|| format!("GET /markets for tag {tag_id}"))?
                .error_for_status()
                .with_context(|| format!("GET /markets for tag {tag_id}"))?
                .json()
                .await
                .context("decoding /markets response")?;
            let Some(page_markets) = body.as_array() else {
                anyhow::bail!("unexpected /markets response shape (not an array)");
            };
            let full_page = page_markets.len() as u32 == PAGE_LIMIT;
            markets.extend(page_markets.iter().filter(|m| tradable(m)).cloned());
            if markets.len() >= max || !full_page {
                break;
            }
        }
        markets.truncate(max);
        Ok(markets)
    }
}

/// Keep only markets tradable on the CLOB (research §1.1): order book enabled
/// and currently accepting orders.
fn tradable(market: &Value) -> bool {
    market.get("enableOrderBook").and_then(Value::as_bool) == Some(true)
        && market.get("acceptingOrders").and_then(Value::as_bool) == Some(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tradable_requires_order_book_and_accepting_orders() {
        let ok = serde_json::json!({"enableOrderBook": true, "acceptingOrders": true});
        assert!(tradable(&ok));
        let no_book = serde_json::json!({"enableOrderBook": false, "acceptingOrders": true});
        assert!(!tradable(&no_book));
        let paused = serde_json::json!({"enableOrderBook": true, "acceptingOrders": false});
        assert!(!tradable(&paused));
        let missing = serde_json::json!({});
        assert!(!tradable(&missing));
    }
}
