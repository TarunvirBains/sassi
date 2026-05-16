//! [`PunnuExecutor`] — internal abstraction over runtime spawn / sleep
//! / now primitives.
//!
//! This is crate-internal in v0.1.0-beta.3. The public runtime surface stays
//! focused on feature selection (`runtime-tokio` or `runtime-wasm`) while the
//! crate keeps scheduling and clock reads behind one internal trait.
//!
//! # Why an internal trait
//!
//! The TTL sweep task and (later) the periodic-refresh helper need
//! `spawn`, `sleep`, and a monotonic clock. Routing those through a
//! trait lets sassi compile cleanly on **both** native (tokio) and
//! `wasm32-unknown-unknown` (gloo-timers + wasm-bindgen-futures)
//! targets without `cfg`-gating every call site.
//!
//! # Three primitives
//!
//! - [`PunnuExecutor::spawn`] — fire-and-forget background task.
//! - [`PunnuExecutor::sleep`] — async sleep on the runtime's timer.
//! - [`PunnuExecutor::now`] — read the monotonic clock. The clock
//!   primitive is part of the executor (rather than a separate
//!   `Clock` trait) because executor-internal cancellation, sleep
//!   anchoring, and TTL bookkeeping all read the same clock; keeping
//!   them on one type avoids a "which clock is which?" confusion.
//!
//! # Test determinism note
//!
//! On native, [`DefaultExecutor::now`] returns
//! [`tokio::time::Instant::now()`] — the paused-clock-aware variant.
//! Sassi's TTL tests use `#[tokio::test(start_paused = true)]` and
//! `tokio::time::advance(...)`; routing reads through the executor
//! preserves that determinism without exposing a tokio-specific knob
//! to consumers. The wasm-target counterpart wraps
//! [`web_time::Instant`] (which uses `Performance.now()` in the
//! browser). The `punnu_executor_wasm` integration test suite exercises
//! spawn, sleep, TTL sweep, periodic refresh, and postcard wire paths under
//! `wasm-bindgen-test`.

use crate::time::Instant;
use std::time::Duration;

// `BoxFut` is the executor's spawn / sleep payload. On native it's
// the `Send + 'static` `BoxFuture` (sassi assumes a multi-threaded
// runtime by default). On wasm it's `LocalBoxFuture` — the wasm
// browser runtime is single-threaded by construction, and several
// wasm-only primitives (`gloo_timers::future::TimeoutFuture`,
// `wasm_bindgen_futures::JsFuture`) hold `!Send` JS callbacks.
// Forcing `Send` would force every wasm sleep through a `Send`
// shim that doesn't add value on a single-threaded runtime.
//
// The split is `cfg(target_arch = "wasm32")` rather than
// `cfg(feature = "runtime-wasm")` because the bound depends on the
// target's threading model, not on which feature flag is selected.
// A native build with `runtime-wasm` enabled (inert per Cargo.toml
// docs) still uses the `Send` bound.
#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
pub(crate) type BoxFut<'a> = futures::future::BoxFuture<'a, ()>;
#[cfg(target_arch = "wasm32")]
#[allow(dead_code)]
pub(crate) type BoxFut<'a> = futures::future::LocalBoxFuture<'a, ()>;

/// Internal abstraction over runtime primitives: `spawn`, `sleep`, and `now`.
///
/// The trait is `Send + Sync` on native (the executor handle gets
/// shared across threads). On wasm it's still `Send + Sync` — the
/// trait object lives in `Arc<dyn PunnuExecutor>` which propagates
/// the bounds — but the futures it produces ([`BoxFut`]) are
/// `!Send` on wasm to allow `gloo_timers` and JS callback closures.
pub(crate) trait PunnuExecutor: Send + Sync {
    /// Spawn a fire-and-forget future. The executor decides which
    /// runtime / thread the future runs on; the caller has no handle
    /// and no cancellation token. Use [`std::sync::Weak`] inside the
    /// spawned future when cancellation must follow owner-loss (see
    /// the TTL sweep in [`crate::punnu::ttl::spawn_sweep`] for the
    /// pattern).
    #[allow(dead_code)]
    fn spawn(&self, fut: BoxFut<'static>);

    /// Async sleep for `duration`. The returned future yields control
    /// to the runtime; on a paused tokio clock (`tokio::time::pause()`
    /// in tests), the sleep advances when the test calls
    /// `tokio::time::advance(...)`. On wasm, the underlying
    /// `gloo_timers::future::TimeoutFuture` uses `setTimeout` so the
    /// sleep advances with browser real time.
    #[allow(dead_code)]
    fn sleep(&self, duration: Duration) -> BoxFut<'static>;

    /// Read the monotonic clock. Returns the executor-appropriate
    /// [`Instant`] type — on native, `tokio::time::Instant` (paused
    /// clock honours `tokio::time::pause()`); on wasm,
    /// `web_time::Instant` (browser `Performance.now()`).
    ///
    /// The wrapper [`crate::time::Instant`] type alias keeps the rest
    /// of sassi free from `cfg(target_arch = "wasm32")` branching at
    /// every clock-read site.
    fn now(&self) -> Instant;
}

/// Default runtime impl. Selected at compile time based on which
/// `runtime-*` feature is active and which target we're compiling for.
///
/// The unit struct carries no state; sassi constructs an `Arc<DefaultExecutor>`
/// at builder time.
pub(crate) struct DefaultExecutor;

#[cfg(all(feature = "runtime-tokio", not(target_arch = "wasm32")))]
impl PunnuExecutor for DefaultExecutor {
    fn spawn(&self, fut: BoxFut<'static>) {
        // `tokio::spawn` requires an active runtime — sassi assumes
        // the consumer is on one (typical native setup). With the
        // sweep task this is the only spawn site; the contract is
        // documented at the call site too.
        tokio::spawn(fut);
    }

    fn sleep(&self, duration: Duration) -> BoxFut<'static> {
        Box::pin(async move { tokio::time::sleep(duration).await })
    }

    fn now(&self) -> Instant {
        // `tokio::time::Instant::now()` is a drop-in for
        // `std::time::Instant::now()` that honours
        // `tokio::time::pause()` / `advance(...)` in tests. Production
        // wall-clock semantics are unchanged.
        tokio::time::Instant::now()
    }
}

#[cfg(all(feature = "runtime-wasm", target_arch = "wasm32"))]
impl PunnuExecutor for DefaultExecutor {
    fn spawn(&self, fut: BoxFut<'static>) {
        wasm_bindgen_futures::spawn_local(fut);
    }

    fn sleep(&self, duration: Duration) -> BoxFut<'static> {
        // `gloo_timers::future::TimeoutFuture` takes milliseconds as
        // a `u32`. Saturate at `u32::MAX` (~49.7 days) — sassi's
        // legitimate sleep durations fit comfortably under that
        // bound; the saturation guards against accidental overflow if
        // a downstream caller passes `Duration::MAX`.
        let ms = duration.as_millis().min(u32::MAX as u128) as u32;
        Box::pin(async move { gloo_timers::future::TimeoutFuture::new(ms).await })
    }

    fn now(&self) -> Instant {
        // `web_time::Instant::now()` reads the browser's
        // `Performance.now()` — a high-resolution monotonic clock
        // suitable for TTL bookkeeping. Native uses
        // `tokio::time::Instant` instead so test pause/advance keeps
        // working.
        web_time::Instant::now()
    }
}

// Fallback impl for the awkward case where neither runtime feature
// is enabled (e.g., `cargo test --no-default-features` without
// `runtime-tokio`). The fallback panics on spawn / sleep — these
// methods are only called when the user asked for a background sweep
// (`PunnuConfig::ttl_sweep_interval = Some(...)`), periodic refresh, or
// delta refresh; the panic is a loud failure that points at the missing
// feature. `now` still works — the same target-aware alias as the
// runtime impls keeps the lazy-expiry path on `Punnu::get` usable
// without any executor feature.
#[cfg(not(any(
    all(feature = "runtime-tokio", not(target_arch = "wasm32")),
    all(feature = "runtime-wasm", target_arch = "wasm32"),
)))]
impl PunnuExecutor for DefaultExecutor {
    fn spawn(&self, _fut: BoxFut<'static>) {
        panic!(
            "PunnuExecutor::spawn called without a runtime feature; \
             enable `runtime-tokio` (native) or `runtime-wasm` (wasm32) \
             to use ttl_sweep_interval / periodic refresh / delta refresh"
        );
    }

    fn sleep(&self, _duration: Duration) -> BoxFut<'static> {
        panic!(
            "PunnuExecutor::sleep called without a runtime feature; \
             enable `runtime-tokio` (native) or `runtime-wasm` (wasm32) \
             to use ttl_sweep_interval / periodic refresh / delta refresh"
        );
    }

    fn now(&self) -> Instant {
        // Same target-aware alias as the runtime impls — see
        // `crate::time::Instant`. Native = `tokio::time::Instant`,
        // wasm = `web_time::Instant`. Tokio's `time` feature is in
        // the workspace dep so `tokio::time::Instant::now()` works
        // without any sassi runtime feature.
        #[cfg(not(target_arch = "wasm32"))]
        {
            tokio::time::Instant::now()
        }
        #[cfg(target_arch = "wasm32")]
        {
            web_time::Instant::now()
        }
    }
}
