//! Kalshi API-key authentication: RSA-PSS-SHA256 request signing.
//!
//! Per docs/research/kalshi-api.md §2, the WebSocket handshake (and any
//! authenticated REST call) requires three headers:
//! - `KALSHI-ACCESS-KEY`: the key id
//! - `KALSHI-ACCESS-TIMESTAMP`: Unix time in milliseconds
//! - `KALSHI-ACCESS-SIGNATURE`: base64 of RSA-PSS(SHA-256, MGF1-SHA256,
//!   salt = digest length) over `"{ts_ms}" + METHOD + PATH` (path without
//!   query params).
//!
//! Credentials come from the environment: `KALSHI_API_KEY_ID` plus either
//! `KALSHI_PRIVATE_KEY_PATH` (PEM file) or `KALSHI_PRIVATE_KEY_PEM` (inline
//! PEM fallback). Key material is never logged or included in errors.

use anyhow::{bail, Context};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use std::fmt;

pub const HEADER_KEY: &str = "KALSHI-ACCESS-KEY";
pub const HEADER_TIMESTAMP: &str = "KALSHI-ACCESS-TIMESTAMP";
pub const HEADER_SIGNATURE: &str = "KALSHI-ACCESS-SIGNATURE";

/// A Kalshi API key id + RSA signing key.
pub struct KalshiCreds {
    key_id: String,
    // `SigningKey::new` uses salt length = digest length (32 for SHA-256),
    // matching Kalshi's documented PSS parameters.
    signing_key: SigningKey<Sha256>,
}

impl fmt::Debug for KalshiCreds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never expose key material; the key id alone is not a secret but is
        // still elided to a prefix.
        f.debug_struct("KalshiCreds")
            .field(
                "key_id",
                &format_args!("{}…", &self.key_id[..self.key_id.len().min(4)]),
            )
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

impl KalshiCreds {
    /// Build from a key id and an RSA private key in PEM form (PKCS#8
    /// `BEGIN PRIVATE KEY` or PKCS#1 `BEGIN RSA PRIVATE KEY`).
    pub fn from_pem(key_id: String, pem: &str) -> anyhow::Result<Self> {
        if key_id.trim().is_empty() {
            bail!("KALSHI_API_KEY_ID is empty");
        }
        let key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            // Deliberately drop the underlying parse error: it can echo
            // fragments of the (secret) input.
            .map_err(|_| {
                anyhow::anyhow!("KALSHI private key is not a valid RSA PEM (PKCS#8 or PKCS#1)")
            })?;
        Ok(KalshiCreds {
            key_id,
            signing_key: SigningKey::new(key),
        })
    }

    /// Load from the environment.
    ///
    /// Returns `Ok(None)` when `KALSHI_API_KEY_ID` is unset (the degraded
    /// no-credential mode), and `Err` when credentials are present but
    /// unusable — that is a configuration error worth surfacing loudly.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let Ok(key_id) = std::env::var("KALSHI_API_KEY_ID") else {
            return Ok(None);
        };
        let pem = match std::env::var("KALSHI_PRIVATE_KEY_PATH") {
            Ok(path) => std::fs::read_to_string(&path)
                .with_context(|| format!("reading KALSHI_PRIVATE_KEY_PATH ({path})"))?,
            Err(_) => std::env::var("KALSHI_PRIVATE_KEY_PEM").context(
                "KALSHI_API_KEY_ID is set but neither KALSHI_PRIVATE_KEY_PATH nor \
                 KALSHI_PRIVATE_KEY_PEM is",
            )?,
        };
        Self::from_pem(key_id, &pem).map(Some)
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Base64 RSA-PSS-SHA256 signature over `"{ts_ms}{method}{path}"`.
    pub fn sign(&self, ts_ms: i64, method: &str, path: &str) -> String {
        let message = format!("{ts_ms}{method}{path}");
        let signature = self
            .signing_key
            .sign_with_rng(&mut rand::thread_rng(), message.as_bytes());
        BASE64.encode(signature.to_bytes())
    }

    /// The three auth headers for a request to `path` (e.g. `/trade-api/ws/v2`),
    /// timestamped now.
    pub fn auth_headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let ts_ms = chrono::Utc::now().timestamp_millis();
        [
            (HEADER_KEY, self.key_id.clone()),
            (HEADER_TIMESTAMP, ts_ms.to_string()),
            (HEADER_SIGNATURE, self.sign(ts_ms, method, path)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::pss::VerifyingKey;
    use rsa::signature::Verifier;

    fn test_key() -> RsaPrivateKey {
        // 2048-bit like real Kalshi keys; generated once per test run.
        RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap()
    }

    #[test]
    fn signature_verifies_with_pss_sha256() {
        let key = test_key();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let creds = KalshiCreds::from_pem("test-key-id".into(), &pem).unwrap();

        let ts_ms = 1_752_800_000_000i64;
        let sig_b64 = creds.sign(ts_ms, "GET", "/trade-api/ws/v2");
        let sig_bytes = BASE64.decode(sig_b64).unwrap();

        let verifier: VerifyingKey<Sha256> = VerifyingKey::new(key.to_public_key());
        let message = format!("{ts_ms}GET/trade-api/ws/v2");
        let signature = rsa::pss::Signature::try_from(sig_bytes.as_slice()).unwrap();
        verifier.verify(message.as_bytes(), &signature).unwrap();
        // A different message must not verify.
        assert!(verifier.verify(b"tampered", &signature).is_err());
    }

    #[test]
    fn auth_headers_have_all_three_fields() {
        let key = test_key();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let creds = KalshiCreds::from_pem("kid-123".into(), &pem).unwrap();
        let headers = creds.auth_headers("GET", "/trade-api/ws/v2");
        assert_eq!(headers[0], (HEADER_KEY, "kid-123".to_string()));
        assert!(headers[1].1.parse::<i64>().unwrap() > 1_700_000_000_000);
        assert!(!headers[2].1.is_empty());
    }

    #[test]
    fn bad_pem_is_rejected_without_echoing_input() {
        let err = KalshiCreds::from_pem(
            "kid".into(),
            "-----BEGIN PRIVATE KEY-----\nsecretsecret\n-----END PRIVATE KEY-----",
        )
        .unwrap_err()
        .to_string();
        assert!(
            !err.contains("secretsecret"),
            "error must not echo key material: {err}"
        );
    }

    #[test]
    fn debug_never_prints_key_material() {
        let key = test_key();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let creds = KalshiCreds::from_pem("verylongkeyid".into(), &pem).unwrap();
        let debug = format!("{creds:?}");
        assert!(debug.contains("<redacted>"));
        assert!(
            !debug.contains("verylongkeyid"),
            "full key id elided: {debug}"
        );
    }
}
