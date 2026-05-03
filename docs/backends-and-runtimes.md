# Backends And Runtimes

Sassi can be used as an in-process cache with no L2 backend. It can also write
through to an L2 backend when persistence or cross-process invalidation is worth
the extra moving parts. Runtime features control only the background work Sassi
needs to spawn: sweep tasks, refresh loops, and backend invalidation listeners.

## L1 And L2

L1 is the `Punnu<T>` resident identity map in the current process. Reads are
cheap and local. Writes publish new immutable snapshots and may evict by TTL,
LRU, or explicit invalidation.

L2 is optional. When attached, it implements `CacheBackend<T>` and receives a
`BackendKeyspace` derived from `PunnuConfig::namespace` plus the Rust type name.
That keyspace is the backend boundary. It does not change the fact that L1 is
one identity map per `Punnu<T>` instance.

L1-only is a valid deployment. For many services, Sassi is valuable as a typed
local resident cache even when all durable truth stays in a database or API.

## Built-In Backends

With the `serde` feature enabled, the core crate includes:

- `MemoryBackend`: an in-memory L2 implementation useful for tests and local
  wiring of the backend path.
- `FileBackend`: a filesystem-backed L2 implementation that stores Sassi wire
  envelopes plus TTL sidecars.

Example:

```rust,no_run
use sassi::{Cacheable, FileBackend, Punnu, PunnuConfig};

#[derive(Cacheable, Clone, Debug, serde::Deserialize, serde::Serialize)]
struct User {
    id: i64,
    name: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _users = Punnu::<User>::builder()
        .config(PunnuConfig {
            namespace: Some("dev".to_owned()),
            ..Default::default()
        })
        .backend(FileBackend::new("./target/sassi-cache"))
        .build();
}
```

Build this with Sassi's `serde` and `runtime-tokio` features enabled. They are
included in the default feature set.

## Redis Companion Crate

Redis lives outside the core crate in `sassi-cache-redis`. The companion crate
provides `RedisBackend<T>` for `CacheBackend<T>` with Redis storage and pub/sub
invalidation.

The backend carries no independent namespace. Keys and channels are derived
from the `BackendKeyspace` Sassi passes in, so `PunnuConfig::namespace` remains
the single source of backend keyspace separation.

```toml
[dependencies]
sassi = "0.1.0-alpha.0"
sassi-cache-redis = "0.1.0-alpha.0"
```

## Backend Failure Modes

`PunnuConfig::backend_failure_mode` defines how strongly the application treats
L2 as part of correctness.

`BackendFailureMode::L1Only` is the default. Backend errors are logged and the
operation succeeds against L1. This is appropriate when L2 is an optimization.

`BackendFailureMode::Retry { attempts }` retries before falling back to L1-only
behavior for retryable failures.

`BackendFailureMode::Error` propagates backend errors. Strict deployments can
use it when a successful cache operation must include L2 write-through or
backend invalidation.

```rust
use sassi::{BackendFailureMode, PunnuConfig};

let config = PunnuConfig {
    backend_failure_mode: BackendFailureMode::Error,
    ..Default::default()
};
```

## Native Runtime

Native background work uses the `runtime-tokio` feature. It is enabled by
default.

Use it when native code attaches a backend, configures `ttl_sweep_interval`, or
starts periodic/delta refresh tasks. Those paths spawn background futures and
require `Punnu::builder().build()` or refresh startup to happen inside an active
Tokio runtime.

If you disable default features for an L1-only library use case, ordinary
construction, `get`, and in-process identity-map behavior still work. Do not
configure background sweep or backend invalidation without a target-compatible
runtime feature.

## WASM Runtime

For `wasm32-unknown-unknown`, enable `runtime-wasm`:

```toml
sassi = {
    version = "0.1.0-alpha.0",
    default-features = false,
    features = ["serde", "runtime-wasm"],
}
```

The WASM executor path uses `wasm-bindgen-futures` for spawn and `gloo-timers`
for sleeps. WASM fetcher traits accept non-`Send` futures so browser-native
futures do not need artificial `Send` wrappers.

The current repository verifies the WASM compile path. Full per-test WASM
runtime execution is tracked separately in issue #3.

## Framework Integrations

Sassi is framework-neutral. A service, worker, CLI, desktop app, or browser WASM
application can own a `Punnu<T>` and decide how it connects to request state,
signals, storage, or networking.

A Dioxus app is one possible downstream consumer of the WASM build. This
repository does not currently certify a Dioxus-specific adapter, signal bridge,
or example. Treat framework integration as application code until a dedicated
adapter exists and is tested here.
