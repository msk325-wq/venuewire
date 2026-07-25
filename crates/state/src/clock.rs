//! Injectable time source so every state component is deterministic under test.
//!
//! The hot path never calls `Utc::now()` directly; [`PipelineState`](crate::PipelineState)
//! reads the clock once per tick and threads the timestamp through book update,
//! match-view freshness, and divergence debounce decisions.

use chrono::{DateTime, Duration, Utc};
use std::fmt;
use std::sync::Mutex;

/// A source of "now". Implementations must be cheap and non-blocking:
/// `now()` is called once per tick on the hot path.
pub trait Clock: Send + Sync + fmt::Debug {
    fn now(&self) -> DateTime<Utc>;
}

/// Production clock: wall time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Test clock: time only moves when the test says so. No wall-clock sleeps.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        *self.now.lock().expect("clock mutex poisoned") = t;
    }

    pub fn advance(&self, d: Duration) {
        let mut now = self.now.lock().expect("clock mutex poisoned");
        *now += d;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().expect("clock mutex poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_advances_only_on_demand() {
        let t0 = Utc::now();
        let clock = ManualClock::new(t0);
        assert_eq!(clock.now(), t0);
        assert_eq!(clock.now(), t0);
        clock.advance(Duration::seconds(5));
        assert_eq!(clock.now(), t0 + Duration::seconds(5));
        clock.set(t0);
        assert_eq!(clock.now(), t0);
    }
}
