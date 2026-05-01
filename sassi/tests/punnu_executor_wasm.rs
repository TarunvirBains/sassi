//! WASM-target executor smoke test (placeholder).
//!
//! Per-test wasm execution is deferred to a future
//! `wasm-bindgen-test` integration — that needs a CI job running
//! `wasm-pack test --node` (or `--headless --chrome`), wiring beyond
//! the runtime-decoupling work that landed sassi's WASM target.
//!
//! The load-bearing wasm verification today is the `wasm-target` CI
//! job in `.github/workflows/ci.yml` — it runs
//! `cargo build -p sassi --target wasm32-unknown-unknown
//! --no-default-features --features "serde,runtime-wasm"` so any
//! regression in the wasm-feature build path fails CI.
//!
//! When wasm-bindgen-test gets wired up, this file's
//! `#[wasm_bindgen_test]` will run a Punnu with `runtime-wasm` +
//! TTL sweep, verifying:
//! 1. `wasm_bindgen_futures::spawn_local` runs the sweep future.
//! 2. `gloo_timers::future::TimeoutFuture` advances on real browser
//!    time.
//! 3. `web_time::Instant` reads `Performance.now()` for TTL deadline
//!    math.
//!
//! For now the file exists as a placeholder (compiled out on native
//! via the cfg gate) so the test path is reserved and discoverable.

#![cfg(target_arch = "wasm32")]

// Body deferred to the wasm-bindgen-test integration — see module
// docs for rationale.
