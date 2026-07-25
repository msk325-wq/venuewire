//! Configuration model. Loaded from a TOML file with `VW_`-prefixed
//! environment variable overrides (e.g. `VW_SERVER__BIND=0.0.0.0:9090`).

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
#[error("failed to load config: {0}")]
pub struct ConfigError(#[from] Box<figment::Error>);

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub ingest: IngestConfig,
    pub kalshi: KalshiConfig,
    pub polymarket: PolymarketConfig,
    pub matcher: MatcherConfig,
    pub divergence: DivergenceConfig,
    pub server: ServerConfig,
    pub redis: RedisConfig,
    pub clickhouse: ClickHouseConfig,
}

impl Config {
    /// Load from `path`, then apply `VW_*` env overrides (`__` separates nesting).
    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("VW_").split("__"))
            .extract()
            .map_err(|e| ConfigError(Box::new(e)))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestConfig {
    pub channel_capacity: usize,
    pub staleness_reconnect_secs: u64,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 10_000,
            staleness_reconnect_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KalshiConfig {
    pub enabled: bool,
    /// Restrict discovery to these series tickers to keep the universe small.
    /// Active series use `KX` prefixes (see docs/research/kalshi-api.md §1).
    pub series_filters: Vec<String>,
    /// REST base URL (unauthenticated market data).
    pub rest_url: String,
    /// WebSocket URL (requires RSA-PSS API-key auth, even for public channels).
    pub ws_url: String,
    /// REST re-poll interval for the degraded no-credential mode.
    pub poll_secs: u64,
}

impl Default for KalshiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            series_filters: vec![],
            rest_url: "https://external-api.kalshi.com/trade-api/v2".into(),
            ws_url: "wss://external-api-ws.kalshi.com/trade-api/ws/v2".into(),
            poll_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketConfig {
    pub enabled: bool,
    /// Gamma tag *slugs* selecting the market universe (Polymarket has no
    /// usable `category` field; discovery filters by tag — see
    /// docs/research/polymarket-api.md §1.1 and decisions.md D8). Slugs are
    /// resolved to numeric tag ids via `GET /tags/slug/{slug}` at startup.
    pub tags: Vec<String>,
    /// Cap on subscribed markets across all tags (highest 24h volume first),
    /// keeping the WSS subscription list sane.
    pub max_markets: usize,
    /// Gamma REST base URL (discovery/metadata; no auth).
    pub gamma_url: String,
    /// CLOB REST base URL (`/book` snapshot refresh; no auth for reads).
    pub clob_url: String,
    /// CLOB WebSocket market-channel URL (no auth).
    pub ws_url: String,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tags: vec![],
            max_markets: 200,
            gamma_url: "https://gamma-api.polymarket.com".into(),
            clob_url: "https://clob.polymarket.com".into(),
            ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MatcherConfig {
    pub refresh_secs: u64,
    /// Feature-gated LLM adjudication; requires ANTHROPIC_API_KEY at runtime.
    pub llm_adjudication: bool,
}

impl Default for MatcherConfig {
    fn default() -> Self {
        Self {
            refresh_secs: 600,
            llm_adjudication: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DivergenceConfig {
    pub threshold: Decimal,
    pub freshness_secs: u64,
    pub debounce_secs: u64,
}

impl Default for DivergenceConfig {
    fn default() -> Self {
        Self {
            threshold: Decimal::new(3, 2), // 0.03
            freshness_secs: 60,
            debounce_secs: 10,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    /// Empty string disables auth (local dev default).
    pub auth_token: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".into(),
            auth_token: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisConfig {
    pub url: String,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseConfig {
    pub url: String,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serial_test::serial;

    // `serial`: Config::load reads process-global VW_* env vars, and the Jail
    // test below mutates them; concurrent runs would race.

    /// The committed default.toml must parse and agree with the in-code defaults.
    #[test]
    #[serial]
    fn shipped_default_toml_parses() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/default.toml");
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.ingest.channel_capacity, 10_000);
        assert_eq!(cfg.ingest.staleness_reconnect_secs, 30);
        assert!(cfg.kalshi.enabled);
        assert_eq!(cfg.kalshi.series_filters, vec!["KXFED", "KXCPI"]);
        assert_eq!(
            cfg.kalshi.rest_url,
            "https://external-api.kalshi.com/trade-api/v2"
        );
        assert_eq!(
            cfg.kalshi.ws_url,
            "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
        );
        assert_eq!(cfg.kalshi.poll_secs, 5);
        assert!(cfg.polymarket.enabled);
        assert_eq!(cfg.polymarket.tags, vec!["politics", "economics"]);
        assert_eq!(cfg.polymarket.max_markets, 200);
        assert_eq!(cfg.polymarket.gamma_url, "https://gamma-api.polymarket.com");
        assert_eq!(cfg.polymarket.clob_url, "https://clob.polymarket.com");
        assert_eq!(
            cfg.polymarket.ws_url,
            "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        );
        assert_eq!(cfg.matcher.refresh_secs, 600);
        assert!(!cfg.matcher.llm_adjudication);
        // TOML float must land exactly on 0.03 as a Decimal
        assert_eq!(cfg.divergence.threshold, dec!(0.03));
        assert_eq!(cfg.server.bind, "0.0.0.0:8080");
        assert!(cfg.server.auth_token.is_empty());
        assert_eq!(cfg.redis.url, "redis://localhost:6379");
        assert_eq!(cfg.clickhouse.url, "http://localhost:8123");
    }

    #[test]
    #[serial]
    fn missing_file_falls_back_to_defaults() {
        // Figment's Toml::file is lenient about missing files; defaults apply.
        let cfg = Config::load(Path::new("/nonexistent/nope.toml")).unwrap();
        assert_eq!(cfg.ingest.channel_capacity, 10_000);
        assert_eq!(cfg.divergence.threshold, dec!(0.03));
    }

    #[test]
    #[serial]
    #[allow(clippy::result_large_err)] // Jail's closure returns figment::Error by value
    fn env_overrides_nested_fields() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "cfg.toml",
                r#"
                [server]
                bind = "0.0.0.0:8080"
                "#,
            )?;
            jail.set_env("VW_SERVER__BIND", "127.0.0.1:9999");
            jail.set_env("VW_INGEST__CHANNEL_CAPACITY", "42");
            let cfg = Config::load(Path::new("cfg.toml")).unwrap();
            assert_eq!(cfg.server.bind, "127.0.0.1:9999");
            assert_eq!(cfg.ingest.channel_capacity, 42);
            Ok(())
        });
    }
}
