//! Heartbeat/staleness watchdog for connector read loops (spec §5.1).
//!
//! Every received frame calls [`Watchdog::touch`]; [`Watchdog::wait_stale`]
//! resolves once no activity has been recorded for the given timeout, so a
//! connector can race it against the socket read and force a reconnect:
//!
//! ```ignore
//! tokio::select! {
//!     frame = ws.next() => { watchdog.touch(); /* handle frame */ }
//!     _ = watchdog.wait_stale(staleness_timeout) => { /* force reconnect */ }
//! }
//! ```
//!
//! Built on `tokio::time`, so tests drive it deterministically with
//! `tokio::time::pause()` / `advance()`.

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// Tracks the last-frame time and detects staleness.
///
/// All methods take `&self` (interior mutability), so `touch` in one
/// `select!` branch coexists with a pending `wait_stale` in another.
#[derive(Debug)]
pub struct Watchdog {
    last_activity: Mutex<Instant>,
}

impl Watchdog {
    /// A watchdog whose activity clock starts now.
    pub fn new() -> Self {
        Watchdog {
            last_activity: Mutex::new(Instant::now()),
        }
    }

    /// Record activity (a frame was received). Resets the staleness clock.
    pub fn touch(&self) {
        *self.last_activity.lock().expect("watchdog lock poisoned") = Instant::now();
    }

    /// The instant of the most recent [`touch`](Watchdog::touch) (or construction).
    pub fn last_activity(&self) -> Instant {
        *self.last_activity.lock().expect("watchdog lock poisoned")
    }

    /// Whether no activity has been recorded for at least `timeout`.
    pub fn is_stale(&self, timeout: Duration) -> bool {
        Instant::now() >= self.last_activity() + timeout
    }

    /// Resolves once no activity has been recorded for `timeout`.
    ///
    /// Concurrent [`touch`](Watchdog::touch) calls push the deadline forward;
    /// the future only completes after a full quiet period.
    pub async fn wait_stale(&self, timeout: Duration) {
        loop {
            let deadline = self.last_activity() + timeout;
            if Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep_until(deadline).await;
        }
    }
}

impl Default for Watchdog {
    fn default() -> Self {
        Watchdog::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn resolves_after_quiet_period() {
        let wd = Watchdog::new();
        let start = Instant::now();
        wd.wait_stale(Duration::from_secs(30)).await;
        assert_eq!(start.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn touch_defers_staleness() {
        let wd = Watchdog::new();
        let start = Instant::now();
        tokio::join!(wd.wait_stale(Duration::from_secs(30)), async {
            // Frames arrive at t=10s, 20s, 30s; the feed then goes quiet.
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                wd.touch();
            }
        });
        // Stale 30s after the last touch at t=30s.
        assert_eq!(start.elapsed(), Duration::from_secs(60));
    }

    #[tokio::test(start_paused = true)]
    async fn already_stale_resolves_immediately() {
        let wd = Watchdog::new();
        tokio::time::advance(Duration::from_secs(45)).await;
        assert!(wd.is_stale(Duration::from_secs(30)));
        let start = Instant::now();
        wd.wait_stale(Duration::from_secs(30)).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn is_stale_tracks_touches() {
        let wd = Watchdog::new();
        let timeout = Duration::from_secs(30);
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!wd.is_stale(timeout));
        wd.touch();
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!wd.is_stale(timeout));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(wd.is_stale(timeout));
    }
}
