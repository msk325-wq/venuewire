//! Kalshi REST market-data client (unauthenticated; see
//! docs/research/kalshi-api.md §1 — market data endpoints require no auth and
//! are CloudFront-cached for ~15s).

use anyhow::Context;
use serde_json::Value;
use std::time::Duration;

/// Page size for `GET /markets` (endpoint max is 1000; 200 keeps individual
/// responses small while our series filters fit in one page anyway).
const PAGE_LIMIT: u32 = 200;

/// Safety cap on pagination depth per series, to stay polite even if a filter
/// unexpectedly matches a huge universe.
const MAX_PAGES: u32 = 10;

#[derive(Debug, Clone)]
pub struct KalshiRest {
    http: reqwest::Client,
    base: String,
}

impl KalshiRest {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("venuewire/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(10))
            .build()
            .context("building Kalshi REST client")?;
        Ok(KalshiRest {
            http,
            base: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// All open markets for one series ticker, following cursor pagination.
    ///
    /// Returns the raw market objects verbatim (normalization happens in
    /// [`super::normalize`], off the recorded frame).
    pub async fn open_markets(&self, series_ticker: &str) -> anyhow::Result<Vec<Value>> {
        let url = format!("{}/markets", self.base);
        let mut markets = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut query: Vec<(&str, String)> = vec![
                ("series_ticker", series_ticker.to_string()),
                ("status", "open".to_string()),
                ("limit", PAGE_LIMIT.to_string()),
            ];
            if let Some(c) = &cursor {
                query.push(("cursor", c.clone()));
            }
            let body: Value = self
                .http
                .get(&url)
                .query(&query)
                .send()
                .await
                .with_context(|| format!("GET /markets for series {series_ticker}"))?
                .error_for_status()
                .with_context(|| format!("GET /markets for series {series_ticker}"))?
                .json()
                .await
                .context("decoding /markets response")?;
            if let Some(page) = body.get("markets").and_then(Value::as_array) {
                markets.extend(page.iter().cloned());
            }
            cursor = body
                .get("cursor")
                .and_then(Value::as_str)
                .filter(|c| !c.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        Ok(markets)
    }
}
