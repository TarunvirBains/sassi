//! Task 10 — WASM-target executor smoke test.
//!
//! **Deferred to Task 18 (CI matrix expansion).** Wiring the
//! `wasm-bindgen-test` runner needs a CI job that runs
//! `wasm-pack test --node` (or `--headless --chrome`); both are
//! outside the scope of Cluster B, where the runtime-decoupling work
//! lives.
//!
//! The load-bearing wasm verification today is the
//! `wasm-target` CI job in `.github/workflows/ci.yml` — it runs
//! `cargo build -p sassi --target wasm32-unknown-unknown
//! --no-default-features --features "serde,runtime-wasm"` so any
//! regression in the wasm-feature build path fails CI.
//!
//! When wasm-bindgen-test is wired up (Task 18), this file's
//! `#[wasm_bindgen_test]` will run a Punnu with `runtime-wasm` +
//! TTL sweep, verifying:
//! 1. `wasm_bindgen_futures::spawn_local` runs the sweep future.
//! 2. `gloo_timers::future::TimeoutFuture` advances on real
//!    browser time.
//! 3. `web_time::Instant` reads `Performance.now()` for TTL
//!    deadline math.
//!
//! For now the file exists as a placeholder (compiled out on
//! native via the cfg gate) so the test path is reserved and
//! discoverable.

#![cfg(target_arch = "wasm32")]

// Body deferred to Task 18 — see module docs for rationale.
