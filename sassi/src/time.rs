//! Cross-target monotonic-clock alias.
//!
//! Sassi reads the clock in three places: TTL deadline computation at
//! insert (`expires_at = now() + ttl`), lazy-expiry comparison on
//! `get` (`expires_at <= now()`), and the background sweep tick. To
//! compile cleanly on both native and `wasm32-unknown-unknown`
//! without `cfg`-branching every call site, sassi exposes a single
//! [`Instant`] type alias that resolves per target.
//!
//! # Native
//!
//! On native (`not(target_arch = "wasm32")`), [`Instant`] is
//! [`tokio::time::Instant`] — a drop-in for
//! [`std::time::Instant`] that honours
//! [`tokio::time::pause()`] / [`tokio::time::advance()`]. This
//! matters for sassi's TTL test suite, which runs under
//! `#[tokio::test(start_paused = true)]` and drives virtual time
//! deterministically. Production wall-clock semantics are unchanged
//! — outside of `pause()`, `tokio::time::Instant` reads the same
//! monotonic clock as `std::time::Instant`.
//!
//! # Wasm
//!
//! On `wasm32-unknown-unknown`, [`Instant`] is
//! [`web_time::Instant`] — a wrapper around the browser's
//! `Performance.now()` API that exposes a `std::time::Instant`-shaped
//! interface. `tokio::time::Instant` is not used on wasm because
//! tokio's timer runtime (and `tokio::time::pause`) doesn't run there;
//! `web_time::Instant` gives wasm consumers a working monotonic clock
//! without any runtime dependency.
//!
//! # Why an alias rather than a wrapper struct
//!
//! Both `tokio::time::Instant` and `web_time::Instant` mirror the
//! `std::time::Instant` API (arithmetic with `Duration`, ordering,
//! `Copy`). Sassi only uses the shared subset, so a type alias is
//! sufficient — wrapping would add boilerplate without buying
//! anything. If a future operation needs target-specific behaviour,
//! we can promote to a newtype then; today the alias is the smaller,
//! more honest choice.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Instant = tokio::time::Instant;

#[cfg(target_arch = "wasm32")]
pub(crate) type Instant = web_time::Instant;
