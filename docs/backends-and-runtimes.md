# Backends And Runtimes

Sassi can be used as an in-process cache with no L2 backend. It can also write
through to an L2 backend when durability or explicit cross-process coordination is
worth the extra moving parts. Runtime features control only the background work
Sassi needs to spawn: sweep tasks, refresh loops, and backend invalidation
listeners.

## L1 And L2

L1 is the `Punnu<T>` resident identity map in the current process. Reads are
cheap and local. Writes publish new immutable snapshots and may evict by TTL,
LRU, or explicit invalidation.

L2 is optional. When attached, it implements `CacheBackend<T>` and receives a
`BackendKeyspace` derived from `PunnuConfig::namespace` plus
`Cacheable::cache_type_name()`. That keyspace is the backend boundary. It does
not change the fact that L1 is one identity map per `Punnu<T>` instance.

L2 can be a durable/shared storage boundary, not automatic write coherence. L2
`put` updates are shared only within the selected backend implementation, while
each process maintains its own L1 state. Cross-process visibility depends on
explicit invalidation and refresh behavior.

For durable or shared L2 data, give cached types an application-owned stable
type name:

```rust
#[derive(sassi::Cacheable, Clone, Debug, serde::Deserialize, serde::Serialize)]
#[cacheable(type_name = "myapp.User")]
struct User {
    id: i64,
    name: String,
}
```

Without an explicit type name, derived and hand-written impls default to Rust's
type path. That is convenient for local caches, tests, and examples, but it is
not a durable schema identifier: a module move or rename changes the backend
keyspace.

Treat explicit type names like durable schema identifiers: they should be unique
within a namespace and reused only when the new Rust type can read the same wire
payloads for the same ids. Reusing a name for incompatible shapes intentionally
points two types at the same L2 keys.

L1-only is a valid deployment. For many services, Sassi is valuable as a typed
local resident cache even when all durable truth stays in a database or API.

## Built-In Backends

With the `serde` feature enabled, the core crate includes:

- `MemoryBackend`: an in-memory L2 implementation useful for tests and local
  wiring of the backend path. It stores Sassi binary wire-container bytes and
  does not publish invalidation streams.
- `FileBackend`: a filesystem-backed L2 implementation that writes `.sassi`
  binary records in beta.2. Each record carries an inline expiry tag followed
  by a postcard-encoded payload, so value and expiry are published with one
  atomic file rename. Beta.1 `.json` records are not read by beta.2 and should
  be treated as disposable cache files. Clear the cache directory or roll
  `PunnuConfig::namespace` during upgrade if stale L2 entries would be noisy.
  It uses blocking filesystem calls inside the async backend trait and is
  intended for development, tests, and simple local persistence rather than
  request-path production load. Like `MemoryBackend`, it does not currently
  publish distributed invalidations.

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

### Custom CacheBackend

The backend trait is small and focused:

- `get(&self, keyspace, id) -> Result<Option<T>, BackendError>`
- `put(&self, keyspace, id, value, ttl) -> Result<(), BackendError>`
- `invalidate(&self, keyspace, id) -> Result<(), BackendError>`
- `invalidate_all(&self, keyspace) -> Result<(), BackendError>`
- `invalidation_stream(&self, keyspace) -> BackendInvalidationStream<T::Id>`

`invalidation_stream` has a no-op default, so backends can opt out of
distributed notifications by using the default behavior.

`MemoryBackend` and `FileBackend` intentionally do not override that method,
so they do not emit distributed invalidations.

`BackendKeyspace` comes from:

- `PunnuConfig::namespace`
- `Cacheable::cache_type_name()`

Backends must treat this pair as the authoritative storage boundary.

## Redis Companion Crate

Redis lives outside the core crate in `sassi-cache-redis`. The companion crate
provides `RedisBackend<T>` for `CacheBackend<T>` with shared Redis storage and
an explicit invalidation pub/sub stream.

The backend carries no independent namespace. Keys and channels are derived
from the `BackendKeyspace` Sassi passes in, so `PunnuConfig::namespace` remains
the single source of backend keyspace separation.

`put` updates Redis values and key index entries, but it does not publish
invalidation events. Cross-process visibility for those writes depends on
publishers calling `invalidate`/`invalidate_all` (or another explicit
invalidation strategy) on the write path, or on applications designing reads to
tolerate stale in-process entries.

`RedisBackend` keeps a per-keyspace index of keys written through the backend,
so `invalidate_all` walks Sassi-managed keys for that namespace/type rather than
scanning an entire shared Redis database. Redis Lua scripts keep value/index
mutations coupled, and all keys for a namespace/type use one Redis Cluster hash
tag so multi-key scripts stay in one slot. TTL-backed index entries expire with
the latest TTL-only data key for that keyspace, and ordinary backend operations
also prune stale index members. In mixed persistent/TTL keyspaces, reads prune
expired TTL members even when a persistent member keeps the index key alive.

`invalidate_all` is still not a quiescence barrier: a concurrent writer in the
same keyspace can write a value during the drain, and that value can survive in
Redis and be read back later. Applications that need a global write barrier
should coordinate writes outside Sassi, move to a new namespace, or use a backend
design with generation tokens.

`invalidate_all` is also best-effort across the delete-and-publish boundary. It
deletes Redis entries in committed batches before publishing the final
keyspace-wide invalidation message. If that final publish fails, Sassi does not
roll back deleted Redis entries, and other processes can retain stale L1 entries
until a later invalidation, refresh, restart, or namespace/generation rollover.

`RedisBackend` drains index keys in bounded chunks, so `invalidate_all` may run
for a long time under sustained concurrent writes.

The Lua scripts call Redis `TIME` to score TTL-backed index members. For
replicated Redis deployments, test this against the Redis version and
replication mode you operate; Redis 7.x deployments are the clearest path for
script effects replication.

```toml
[dependencies]
sassi = "0.1.0-beta.2"
sassi-cache-redis = "0.1.0-beta.2"
```

## Shared L2 Upgrade From Beta.1

Persistent or shared L2 backends that store Sassi wire bytes must also be
cleared or moved to a new `PunnuConfig::namespace` before beta.2 readers attach.
`FileBackend` treats beta.1 `.json` files as cold misses because the extension
changes to `.sassi` in beta.2; stray `.json` files are ignored on read.

Redis and custom backends keep their existing keys, so beta.1 JSON values would
decode as wire-format errors under beta.2. With the default
`BackendFailureMode::L1Only`, a backend decode error is logged and treated as a
miss on `get_async`; with `BackendFailureMode::Error`, it is returned to the
caller. Operators using shared Redis should clear the keyspace or roll
`PunnuConfig::namespace` during upgrade so beta.2 readers do not attempt to
decode beta.1 JSON values.

The final commit where the beta.1 JSON value envelope was live is
`92b77510cb80d98fd749020df3d18571200a315f`; use
`git show 92b77510cb80d98fd749020df3d18571200a315f:sassi/src/wire.rs` if an
upgrade tool needs the exact historical decoder.

## Local Snapshots vs Shared Backend Mutation

Sassi distinguishes two cache surfaces that beta.2 supports concurrently:
shared L2 mutation through async backends, and local L1 hydration through
`Punnu::export_entries_postcard` / `Punnu::restore_entries_postcard`. They are
not interchangeable.

Service-side pools with Redis or other shared backends should keep shared L2
state on the existing async backend paths. Do not use this kind of pool for
local snapshot restore:

```rust,ignore
let pool = Punnu::<ProfileDoc>::builder()
    .backend(RedisBackend::new(redis_client))
    .build();

pool.insert(profile).await?;
pool.invalidate(&profile_id, InvalidationReason::OnSave).await?;
```

Frontend, mobile, edge, or request-local pools without an attached L2 backend
can hydrate local continuity state by loading bytes from platform storage
asynchronously, then restoring the already-loaded entries into local L1
synchronously:

```rust,ignore
let proposal_pool = Punnu::<ProposalPreview>::builder().build();
let bytes = local_store.load("proposal-cache").await?;
proposal_pool.restore_entries_postcard(&bytes)?;

let bytes = proposal_pool.export_entries_postcard()?;
local_store.save("proposal-cache", bytes).await?;
```

`restore_entries_postcard` rejects when strict backend writes are in flight. In
beta.2, "strict" means an active backend write reservation from a pool using
`BackendFailureMode::Error`; this is a race guard, not an enforcement mechanism
for the broader rule above. The intended beta.2 restore path is backend-less
local hydration; backend seeding remains a future async API rather than a
widening of `restore_entries_postcard`.

The snapshot is not a distributed correctness boundary. Applications that need
multi-device or service-to-service recovery should pair entries snapshots with
their own server-confirmed cursors, generations, or event-log positions.

Examples should use projection or view `Cacheable` types rather than
persistence models. Backend code decides which projection a caller may see,
serializes that projection into the entries snapshot, and never exposes
private fields merely because they exist on a database model.

## Backend Failure Modes

`PunnuConfig::backend_failure_mode` defines how strongly the application treats
L2 as part of correctness.

`BackendFailureMode::L1Only` is the default. Backend errors are logged and the
operation succeeds against L1. This is appropriate when L2 is an optimization.

`BackendFailureMode::Retry { attempts }` retries before falling back to L1-only
behavior for retryable failures.

`BackendFailureMode::Error` propagates errors for operations that actually
touch the backend. Today that means `insert` write-through, `get_async` backend
reads, and `invalidate` backend invalidation. Fetch and refresh helpers such as
`get_or_fetch`, `get_or_fetch_many`, `apply_delta`, `start_periodic_refresh`,
and `start_delta_refresh` apply fetched values to the in-process L1 map; they do
not write those fetched values through to L2 or publish L2 invalidations for
query membership changes.

In best-effort modes (`L1Only` and exhausted `Retry`), a failed single-id
backend invalidation suppresses future `get_async` L2 rehydration for that id in
the same process. The suppression is set before the local L1 entry is removed,
so concurrent same-id `get_async` calls cannot rehydrate L2 while backend
invalidation is in flight. Suppression is cleared after a later local backend
write or local backend invalidation for that id succeeds. Backend invalidation
stream delivery does not clear it because streams do not carry an ordering token
relative to this process's failed invalidation. Suppression is per-id and is not
capped by the L1 `lru_size`, so a prolonged backend outage can retain one local
suppression entry per failed invalidation. This prevents a local pool from
resurrecting a value it explicitly invalidated during an L2 outage, but it is not
a distributed generation-token guarantee for other processes.

### L1/L2 Access Boundaries

`get` is L1-only.

`get_async` checks L1 first, then backend, and finally inserts the backend value
into L1 if canonical. It skips the backend read when a prior best-effort
invalidation failure marked that id as locally untrusted.

`get_or_fetch` and `get_or_fetch_many` are canonical-id fetch helpers. They do
not `put` back to L2, and they should not be used as query/page-style membership
sources.

`get_or_fetch_many` does not promise input-order output.

### Wire Ingress and TTL

`insert_serialized` expects bytes produced by `sassi::wire::to_vec`:

```rust
let bytes = sassi::wire::to_vec(&value)?;
let decoded = sassi::wire::from_slice::<User>(&bytes)?;
pool.insert_serialized(&bytes).await?;
```

Sassi's `serde` feature uses a postcard-backed binary container for value wire.
The header carries Sassi magic bytes, wire major `1`, a kind byte, zero flags,
and `Cacheable::cache_type_name()` before postcard payload bytes. Readers
validate the header before decoding the typed payload, so an incompatible
major, kind, type, or flags is rejected before the payload is ever touched.

The wire bytes carry only the binary header plus the payload; they do not
embed TTL policy. TTL remains local (`insert`, `insert_with_ttl`,
`PunnuConfig::default_ttl`, etc.).

Sassi's own wire metadata uses fixed-width integer fields. If cached payloads
will cross native/wasm or long-lived storage boundaries, model payload integers
with explicit widths (`u32`, `u64`, `i32`, `i64`) rather than `usize` or
`isize`, whose serialized meaning can differ across target pointer widths.

### Pool-wide Reset

There is no public `Punnu::clear` or `Punnu::invalidate_all`.

- For local process memory reset, create a new `Punnu<T>` and drop the old pool.
- For cross-process intent, call backend `invalidate_all` and keep a
  listener-attached process subscribed to it so local pools can react.

```rust
use sassi::{BackendFailureMode, PunnuConfig};

let config = PunnuConfig {
    backend_failure_mode: BackendFailureMode::Error,
    ..Default::default()
};
```

## Runtime and Builder Guardrails

- `lru_size` must be non-zero.
- `event_channel_capacity` must be greater than zero.
- `retry` mode requires `attempts >= 1`.
- non-empty string is required when `namespace = Some(...)`.
- `ttl_sweep_interval = Some(Duration::ZERO)` is rejected.
- attaching a backend or configuring a sweep requires an active runtime-aware build
  and target-compatible runtime feature when `build` is called.
- the default feature set uses `runtime-tokio`; for `wasm32-unknown-unknown` builds
  disable defaults and enable `runtime-wasm`.

Native default behavior uses `runtime-tokio`.

For `wasm32-unknown-unknown`, enable `runtime-wasm`.

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
    version = "0.1.0-beta.2",
    default-features = false,
    features = ["serde", "runtime-wasm"],
}
```

The WASM executor path uses `wasm-bindgen-futures` for spawn and `gloo-timers`
for sleeps. WASM fetcher traits accept non-`Send` futures so browser-native
futures do not need artificial `Send` wrappers.

The current repository verifies both the WASM compile path *and* the
`runtime-wasm` execution path. The `wasm-target` CI job runs
`wasm-bindgen-test` integration tests under node, exercising
`wasm_bindgen_futures::spawn_local`, `gloo_timers::future::TimeoutFuture`,
and `web_time::Instant` arithmetic against the same `Punnu<T>` surface that
native consumers use.

## Framework Integrations

Sassi is framework-neutral. A service, worker, CLI, desktop app, or browser WASM
application can own a `Punnu<T>` and decide how it connects to request state,
signals, storage, or networking.

A Dioxus app is one possible downstream consumer of the WASM build. This
repository does not currently certify a Dioxus-specific adapter, signal bridge,
or example. Treat framework integration as application code until a dedicated
adapter exists and is tested here.
