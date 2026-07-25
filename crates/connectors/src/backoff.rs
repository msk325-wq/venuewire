//! Jittered exponential backoff for reconnect loops (spec §5.1).
//!
//! Delays grow `base, 2·base, 4·base, …` up to `cap`, and each returned delay
//! is *full-jittered*: drawn uniformly from `[0, current]`. Call
//! [`Backoff::reset`] after a successful connect so the next failure starts
//! from `base` again.

use std::fmt;
use std::time::Duration;

use rand::Rng;

/// Default base delay per spec §5.1.
pub const DEFAULT_BASE: Duration = Duration::from_millis(500);

/// Default delay cap per spec §5.1.
pub const DEFAULT_CAP: Duration = Duration::from_secs(30);

/// Maps the current (un-jittered) delay to the delay actually slept.
type JitterFn = Box<dyn FnMut(Duration) -> Duration + Send>;

/// Jittered exponential backoff schedule.
///
/// Also an infinite [`Iterator`] over delays, so a reconnect loop can either
/// call [`next_delay`](Backoff::next_delay) explicitly or treat it as a
/// stream of sleep durations.
pub struct Backoff {
    base: Duration,
    cap: Duration,
    current: Duration,
    jitter: JitterFn,
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("base", &self.base)
            .field("cap", &self.cap)
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

impl Backoff {
    /// Backoff with full jitter: each delay is uniform in `[0, current]`.
    pub fn new(base: Duration, cap: Duration) -> Self {
        Self::with_jitter(base, cap, Box::new(full_jitter))
    }

    /// Backoff with a caller-supplied jitter function. Pass the identity to
    /// make the schedule fully deterministic (used by tests).
    pub fn with_jitter(base: Duration, cap: Duration, jitter: JitterFn) -> Self {
        Backoff {
            base,
            cap,
            current: base,
            jitter,
        }
    }

    /// The next delay to sleep before reconnecting; advances the schedule.
    pub fn next_delay(&mut self) -> Duration {
        let delay = (self.jitter)(self.current);
        self.current = self
            .current
            .checked_mul(2)
            .unwrap_or(self.cap)
            .min(self.cap);
        delay
    }

    /// Reset to `base`. Call after a successful connect.
    pub fn reset(&mut self) {
        self.current = self.base;
    }

    /// The un-jittered delay the next [`next_delay`](Backoff::next_delay)
    /// call will draw from.
    pub fn current(&self) -> Duration {
        self.current
    }
}

impl Default for Backoff {
    /// Spec defaults: base 500ms, cap 30s, full jitter.
    fn default() -> Self {
        Backoff::new(DEFAULT_BASE, DEFAULT_CAP)
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        Some(self.next_delay())
    }
}

/// Full jitter: uniform in `[0, current]` (AWS-style), so simultaneous
/// reconnecting clients decorrelate instead of thundering back together.
fn full_jitter(current: Duration) -> Duration {
    let max = u64::try_from(current.as_nanos()).unwrap_or(u64::MAX);
    Duration::from_nanos(rand::thread_rng().gen_range(0..=max))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deterministic(base: Duration, cap: Duration) -> Backoff {
        Backoff::with_jitter(base, cap, Box::new(|d| d))
    }

    #[test]
    fn grows_exponentially_from_base() {
        let mut b = deterministic(Duration::from_millis(500), Duration::from_secs(30));
        assert_eq!(b.next_delay(), Duration::from_millis(500));
        assert_eq!(b.next_delay(), Duration::from_millis(1000));
        assert_eq!(b.next_delay(), Duration::from_millis(2000));
        assert_eq!(b.next_delay(), Duration::from_millis(4000));
    }

    #[test]
    fn caps_and_stays_capped() {
        let mut b = deterministic(Duration::from_millis(500), Duration::from_secs(30));
        let delays: Vec<_> = b.by_ref().take(10).collect();
        assert_eq!(delays[6], Duration::from_secs(30)); // 500ms * 2^6 = 32s -> capped
        assert_eq!(delays[7], Duration::from_secs(30));
        assert_eq!(delays[9], Duration::from_secs(30));
        assert_eq!(b.current(), Duration::from_secs(30));
    }

    #[test]
    fn reset_returns_to_base() {
        let mut b = deterministic(Duration::from_millis(500), Duration::from_secs(30));
        for _ in 0..8 {
            b.next_delay();
        }
        assert_eq!(b.current(), Duration::from_secs(30));
        b.reset();
        assert_eq!(b.next_delay(), Duration::from_millis(500));
        assert_eq!(b.next_delay(), Duration::from_millis(1000));
    }

    #[test]
    fn full_jitter_stays_within_bounds() {
        // The default (random) jitter must always land in [0, current].
        let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        let mut expected = Duration::from_millis(500);
        for _ in 0..200 {
            let uncapped = b.current();
            assert_eq!(uncapped, expected);
            let delay = b.next_delay();
            assert!(delay <= uncapped, "jittered {delay:?} > {uncapped:?}");
            expected = (expected * 2).min(Duration::from_secs(30));
        }
    }

    #[test]
    fn jitter_fn_receives_current_delay() {
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let mut b = Backoff::with_jitter(
            Duration::from_millis(100),
            Duration::from_millis(400),
            Box::new(move |d| {
                recorder.lock().unwrap().push(d);
                Duration::ZERO
            }),
        );
        assert_eq!(b.next_delay(), Duration::ZERO);
        assert_eq!(b.next_delay(), Duration::ZERO);
        assert_eq!(b.next_delay(), Duration::ZERO);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400)
            ]
        );
    }
}
