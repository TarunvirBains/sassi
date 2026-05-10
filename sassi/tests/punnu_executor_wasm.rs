//! WASM-target integration tests for sassi's `runtime-wasm` executor.
//!
//! This file is compiled only on `wasm32-unknown-unknown`. It exercises
//! the same `Punnu<T>` surface adopters use, but on the
//! [`wasm-bindgen-futures`] / [`gloo-timers`] executor instead of
//! tokio. The goal is to prove that:
//!
//! 1. [`crate::executor::PunnuExecutor::spawn`] (backed by
//!    `wasm_bindgen_futures::spawn_local`) actually runs a future to
//!    completion under wasm.
//! 2. [`crate::executor::PunnuExecutor::sleep`] (backed by
//!    `gloo_timers::future::TimeoutFuture`) advances real browser /
//!    node time.
//! 3. [`crate::executor::PunnuExecutor::now`] (backed by
//!    [`web_time::Instant`], which wraps `Performance.now()`) is a
//!    monotonic clock suitable for Sassi's TTL deadline arithmetic.
//!
//! The tests intentionally use small absolute durations and skip
//! tight-bound timing assertions: under `wasm-pack test --node`, the
//! runner's event loop is not as well-controlled as native
//! `tokio::time::pause()` / `advance()`, so we assert structural
//! outcomes (entry expired, sweep removed it, refresh updated state)
//! rather than millisecond-precise schedules.
//!
//! ## Running locally
//!
//! ```text
//! cargo install wasm-pack
//! wasm-pack test --node sassi --no-default-features \
//!     --features serde,runtime-wasm
//! ```
//!
//! The `wasm-pack test` runner expects the crate's `[lib]` section to
//! be `cdylib`-compatible. Sassi is a plain library; the runner uses
//! the integration test as its own entrypoint via
//! `wasm_bindgen_test_configure!`.

#![cfg(target_arch = "wasm32")]
#![cfg(all(feature = "serde", feature = "runtime-wasm"))]

use sassi::{Cacheable, Field, Punnu, PunnuConfig, PunnuFetcher, RefreshMode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use wasm_bindgen_test::*;

// Default runner is node; explicit `run_in_browser` would require a
// headless browser like Chrome or Firefox. Sassi's wasm tests assert
// on structural outcomes that do not depend on a DOM, so node is the
// lighter and more deterministic CI environment.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Item {
    id: i64,
    label: String,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    id: Field<Item, i64>,
}

impl Cacheable for Item {
    type Id = i64;
    type Fields = ItemFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> ItemFields {
        ItemFields {
            id: Field::new("id", |item| &item.id),
        }
    }
}

/// A small async sleep helper that uses the same gloo-timers primitive
/// the executor relies on. Tests use this to wait for the executor's
/// background work without coupling to `tokio::time::sleep` (which
/// does not run on wasm).
async fn sleep(ms: u32) {
    gloo_timers::future::TimeoutFuture::new(ms).await;
}

/// Round-trip: an entry inserted into a wasm `Punnu` is readable from
/// the same pool. This proves the executor builder path runs to
/// completion under `wasm_bindgen_futures::spawn_local` (insert is
/// async).
#[wasm_bindgen_test]
async fn insert_then_get_round_trips_under_runtime_wasm() {
    let pool = Punnu::<Item>::builder().build();
    pool.insert(Item {
        id: 7,
        label: "seven".into(),
    })
    .await
    .unwrap();

    let entry = pool.get(&7).expect("inserted entry must be readable");
    assert_eq!(entry.label, "seven");
}

/// Lazy TTL expiry on the wasm clock: an entry inserted with a short
/// TTL becomes invisible to `get` after a real-time sleep. Proves
/// `web_time::Instant` arithmetic behaves correctly for
/// `expires_at = now + ttl` and `expires_at <= now()` comparisons under
/// the `runtime-wasm` executor.
#[wasm_bindgen_test]
async fn insert_with_ttl_lazily_expires_under_wasm_clock() {
    let pool = Punnu::<Item>::builder().build();
    pool.insert_with_ttl(
        Item {
            id: 1,
            label: "short".into(),
        },
        Duration::from_millis(20),
    )
    .await
    .unwrap();

    assert!(
        pool.get(&1).is_some(),
        "entry must be readable before TTL elapses"
    );

    sleep(60).await;

    assert!(
        pool.get(&1).is_none(),
        "entry must read as absent after wasm-clock TTL elapses"
    );
}

/// TTL sweep task fires under the wasm executor: with a configured
/// sweep interval, expired entries are physically removed from L1
/// without an additional reader bump. Proves the
/// `wasm_bindgen_futures::spawn_local` path actually drives the sweep
/// future to completion.
#[wasm_bindgen_test]
async fn ttl_sweep_runs_under_runtime_wasm() {
    let pool = Punnu::<Item>::builder()
        .config(PunnuConfig {
            ttl_sweep_interval: Some(Duration::from_millis(20)),
            ..Default::default()
        })
        .build();
    pool.insert_with_ttl(
        Item {
            id: 1,
            label: "expires".into(),
        },
        Duration::from_millis(10),
    )
    .await
    .unwrap();

    // Wait long enough for one or more sweep ticks to fire after the
    // entry's TTL has elapsed. The bound is generous to absorb
    // node/browser timer jitter.
    sleep(150).await;

    assert!(
        pool.get(&1).is_none(),
        "sweep task must remove the expired entry from L1"
    );
    // After a sweep removed the only entry, `len` reports zero
    // synchronously. This catches the case where the sweep would have
    // marked the entry expired lazily but never published the cleaned
    // snapshot.
    assert_eq!(
        pool.len(),
        0,
        "sweep task must publish a snapshot that drops the expired entry"
    );
}

/// User-supplied fetcher under the wasm `?Send` trait shape. Proves
/// the wasm-side `PunnuFetcher` trait wires through to a periodic
/// refresh handle that drives spawn + sleep on
/// `wasm_bindgen_futures::spawn_local` + `gloo_timers`.
struct CountingFetcher {
    label: &'static str,
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait(?Send)]
impl PunnuFetcher<Item> for CountingFetcher {
    async fn fetch(&self) -> Result<Vec<Item>, sassi::FetchError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![Item {
            id: 1,
            label: format!("{}-{n}", self.label),
        }])
    }
}

/// Periodic refresh under the wasm executor: at least one fetcher call
/// fires, and its returned value reaches L1. This is the simplest path
/// that exercises spawn (refresh task) + sleep (refresh tick) +
/// snapshot publish on the wasm runtime.
#[wasm_bindgen_test]
async fn periodic_refresh_runs_under_runtime_wasm() {
    let pool = Punnu::<Item>::builder().build();
    let calls = Arc::new(AtomicUsize::new(0));
    let _handle = pool.start_periodic_refresh(
        Duration::from_millis(20),
        CountingFetcher {
            label: "wasm",
            calls: calls.clone(),
        },
        RefreshMode::UpsertOnly,
    );

    // Wait for at least one tick. Generous to absorb gloo-timers
    // scheduling jitter under node.
    sleep(200).await;

    assert!(
        calls.load(Ordering::SeqCst) >= 1,
        "periodic refresh fetcher must fire at least once under runtime-wasm"
    );
    let entry = pool
        .get(&1)
        .expect("refreshed entry must be visible in L1 after the tick");
    assert!(
        entry.label.starts_with("wasm-"),
        "L1 entry should reflect the refreshed value"
    );
}

/// Exercise the entries-only postcard snapshot/restore round-trip on
/// the wasm executor. Provides cheap coverage of the `sassi::wire`
/// path under `runtime-wasm` so a wasm-side wire-format regression is
/// caught in CI.
#[wasm_bindgen_test]
async fn snapshot_postcard_round_trips_under_runtime_wasm() {
    let pool = Punnu::<Item>::builder().build();
    pool.insert(Item {
        id: 1,
        label: "alpha".into(),
    })
    .await
    .unwrap();
    pool.insert(Item {
        id: 2,
        label: "beta".into(),
    })
    .await
    .unwrap();

    let bytes = pool.export_entries_postcard().unwrap();

    let restored = Punnu::<Item>::builder().build();
    let stats = restored.restore_entries_postcard(&bytes).unwrap();

    assert_eq!(stats.inserted, 2);
    assert_eq!(restored.get(&1).unwrap().label, "alpha");
    assert_eq!(restored.get(&2).unwrap().label, "beta");
}
