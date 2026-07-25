//! HTTP transport to ClickHouse, behind a trait so the batching writer is
//! testable without a live server.
//!
//! ClickHouse's HTTP interface takes the SQL statement as the `query` URL
//! parameter and the data (JSONEachRow lines for inserts, empty for DDL) as
//! the POST body. See decisions draft D-M5-2 for why we use JSONEachRow over
//! HTTP instead of the `clickhouse` crate's RowBinary.

use async_trait::async_trait;
use std::fmt;
use std::time::Duration;

/// A transport failure. `Status` carries the (truncated) ClickHouse error
/// body, which contains the actual parse/type error text — invaluable when
/// debugging schema drift.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("clickhouse http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("clickhouse returned status {code}: {body}")]
    Status { code: u16, body: String },
    #[error("invalid clickhouse url: {0}")]
    Url(String),
    /// Test-injected failures.
    #[error("{0}")]
    Other(String),
}

/// Executes one ClickHouse statement. `query` is the SQL (INSERT ... FORMAT
/// JSONEachRow, or DDL); `body` is the newline-joined JSONEachRow payload
/// (empty for DDL).
#[async_trait]
pub trait Transport: Send + Sync + fmt::Debug {
    async fn execute(&self, query: &str, body: String) -> Result<(), TransportError>;
}

/// Production transport: `POST {url}/?query=...&database=...` via reqwest.
#[derive(Debug, Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
    url: reqwest::Url,
    database: String,
}

impl HttpTransport {
    /// `url` is the ClickHouse HTTP endpoint (e.g. `http://localhost:8123`).
    pub fn new(url: &str, database: &str, timeout: Duration) -> Result<Self, TransportError> {
        let url = reqwest::Url::parse(url).map_err(|e| TransportError::Url(e.to_string()))?;
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(TransportError::Http)?;
        Ok(Self {
            client,
            url,
            database: database.to_string(),
        })
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn execute(&self, query: &str, body: String) -> Result<(), TransportError> {
        let resp = self
            .client
            .post(self.url.clone())
            .query(&[("query", query), ("database", &self.database)])
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let mut body = resp.text().await.unwrap_or_default();
            body.truncate(500);
            return Err(TransportError::Status {
                code: status.as_u16(),
                body,
            });
        }
        Ok(())
    }
}
