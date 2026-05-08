# Concepts

Sassi is a typed cache substrate. It gives application code a clear place for
identity, in-memory predicates, refresh state, backend write-through, and
cross-type orchestration without pretending to be the database or framework
above it.

## Cacheable

`Cacheable` is the identity contract for values stored in a `Punnu<T>`.
`T::Id` is the cache key, and `T::fields()` returns field accessors that power
`BasicPredicate<T>`.

Most adopters use `#[derive(sassi::Cacheable)]`:

```rust
use sassi::Cacheable;

#[derive(Cacheable, Clone, Debug)]
struct Invoice {
    id: i64,
    customer_id: i64,
    status: String,
}
```

The `id` must be stable for the lifetime of the value. Mutating a cached value
so that its identity changes is a logic bug: the pool stores one canonical
value per `T::Id`.

For delta sync, the derive can also mark a monotonic cursor:

```rust
use sassi::Cacheable;

#[derive(Cacheable, Clone, Debug)]
#[cacheable(watermark_field = "updated_at")]
struct Invoice {
    id: i64,
    updated_at: i64,
    status: String,
}
```

That generates `DeltaSyncCacheable` when the watermark type implements
`MonotonicWatermark`.

Composite cursors are also supported for tuple shapes via `MonotonicWatermark`
marker impls:

```rust
type Cursor2 = (i64, u64);
type Cursor3 = (i64, u64, std::time::SystemTime);
type Cursor4 = (i64, u64, std::time::SystemTime, std::time::SystemTime);
```

One-element and >4-element tuples are not marker-compatible in this release.

## Punnu<T>

`Punnu<T>` is the typed in-process pool. It is a resident union identity map:
one shared L1 for values of `T`, keyed by `Cacheable::Id`.

It is not one materialized query result set. Several fetchers or refreshers can
write into the same `Punnu<T>`, and the pool keeps the resident union of
canonical identities subject to TTL, LRU, invalidation, and conflict policy.
Query-specific reads are expressed with predicates at read time.

Cloning a `Punnu<T>` is cheap. Clones share the same inner state, event stream,
configuration, and optional backend. Reads load immutable snapshots; writes
publish a new snapshot after preparing the change.

`PunnuConfig::namespace` separates backend keyspaces. It does not create a
separate in-process L1 map. If two tenants share one `Punnu<User>` and both use
`id = 1`, they are the same cached identity unless the type or id encodes the
tenant.

Tenant-aware process wiring should stay explicit:

- Use tenant-qualified ids when identity itself is tenant-scoped.
- Use wrapper types when each model family has a distinct tenant policy.
- Carry `TenantKey` only as request context. It does not change L1 identity or
  single-flight behavior.

`PunnuConfig::namespace` changes only backend keys/channels, never L1 isolation.
If two tenants can share a concrete id in one process, encode that tenant in the
cache id or keep tenant-separate pools.

## BasicPredicate<T>

`BasicPredicate<T>` is the inspectable field-predicate algebra. Field accessors
build predicates such as `eq`, `gte`, `in_`, `contains`, and `is_null`.
Predicates compose with `&`, `|`, `^`, and `!`, and Sassi can evaluate them
against resident values.

A `BasicPredicate<T>` is walkable and projectable by in-process consumers:
callers can inspect field names, lookup operators, and typed operand values.
That makes it a useful bridge between a data-layer fetcher and Sassi's
in-memory replay.

It is not a serde wire format in v0.1.0. Persisting or transmitting predicate
values across processes needs an application or downstream crate to define a
typed codec.

## MemQ<T>

`MemQ<T>` is Sassi's in-memory query pipeline. It can wrap a
`BasicPredicate<T>`, but it also supports local operations such as closure
filters, map, flat-map, sort, take, skip, unique, group, partition, and fold.

`MemQ<T>` is intentionally memory-only. Closure filters and mappers are useful
after data is resident, but they cannot be lowered to SQL or serialized. When a
query has to be understood by a source-of-truth fetcher, keep the portable part
in `BasicPredicate<T>` and treat `MemQ<T>` as local post-processing.

## Refreshers

Refreshers are background tasks that fetch values and apply them to a pool.
They carry explicit fetcher, filter, progress, and recovery state outside the
identity map.

`start_periodic_refresh` handles simple fixed-interval polling. With
`RefreshMode::UpsertOnly`, fetched values are inserted or updated and absent
resident ids are left alone. With `RefreshMode::Replace`, the fetched set is
treated as authoritative for the whole `Punnu<T>`.

That distinction is a release-critical boundary. `Replace` is appropriate only
when the fetcher returns the complete truth set for the whole resident pool.
For partial, tenant-filtered, auth-filtered, or paginated pollers, prefer
`UpsertOnly` or isolate the cache identity with a wrapper type or qualified id.

### Delta Refresh Handle

`start_delta_refresh` returns a `DeltaRefreshHandle<T>` you can drive from
application flow:

```rust
use std::num::NonZeroUsize;

let handle = users.start_delta_refresh(std::time::Duration::from_secs(10), fetcher);
let _ = handle.update().await?;
let _ = handle.update_full().await?;
let handle = handle
    .with_eviction_recovery(true)
    .with_periodic_full_refresh(Some(NonZeroUsize::new(100).unwrap()));
```

- `update()` runs a regular delta tick.
- `update_full()` forces a full-refresh tick (`since = None`), and is queued if a
  delta tick is already in flight.
- `pending_eviction_recovery_count()` and
  `periodic_full_refresh_progress()` expose recovery and periodic scheduling.
- `watermark()` returns the subscription cursor.

## Delta Sync And Watermarks

`DeltaSyncCacheable` extends `Cacheable` with a monotonic watermark. A delta
refresh subscription owns its own watermark and in-flight update slot, so a
narrow subscription cannot advance a broader subscription's progress.

Delta fetchers receive `DeltaQuery<T>`, including:

- `since: Option<T::Watermark>`, where `None` means full query.
- `recover_ids`, used by eviction recovery to ask for specific identities.

When `since` is `Some`, fetchers must treat it as an inclusive `>=` lower
bound. Boundary rows may have changed without their watermark changing; Sassi
deduplicates by identity when those rows are returned again.

`DeltaResult<T>` contains items to upsert and tombstones for true deletes.
Absence from `items` never deletes a resident entry. Tombstones delete from the
shared identity map, so they should mean "deleted from the source of truth",
not "left this query". For delete-only progress, use
`DeltaResult::with_high_watermark`.

You can apply a batch directly with `Punnu::apply_delta` when the same process is
already producing a delta source result:

```rust
use std::collections::HashSet;

let stats = users.apply_delta(DeltaResult::new(changed_users, HashSet::new()));
if stats.backend_reserved_skips > 0 {
    // one or more ids were blocked by strict L2 in-flight insert reservations
}
```

`backend_reserved_skips` means that authoritative delta application should be
retried after backend reservations clear.

## Backends

The core crate has an L1 in-process map and optional L2 `CacheBackend`.
With `serde` enabled, the built-in L2 backends are:

- `MemoryBackend`, mainly useful for tests and local wiring.
- `FileBackend`, useful for development and simple local persistence.

Redis lives in the `sassi-cache-redis` companion crate. L1-only operation is a
valid deployment shape; attach L2 only when the extra persistence or
cross-process invalidation is worth the operational cost.

Backend failures are policy-controlled. The default `BackendFailureMode::L1Only`
keeps the cache useful when L2 is down. Strict deployments can choose
`BackendFailureMode::Error` when backend-touching operations should fail rather
than quietly degrading to L1. That policy covers operations that actually call
the backend, such as `insert`, `get_async`, and `invalidate`; fetch and refresh
helpers apply their fetched values to L1 and keep query membership changes out
of the L2 invalidation channel.

## Events and Invalidation Reasons

`Punnu::events()` returns a broadcast receiver for local observability.
It is intentionally lossy: if one subscriber falls behind the configured
`event_channel_capacity`, it gets `RecvError::Lagged`, and only newer events
remain. The producer side never blocks.

```rust
use tokio::sync::broadcast::error::RecvError;

let mut rx = users.events();
loop {
    match rx.recv().await {
        Ok(sassi::PunnuEvent::Invalidate { id, reason }) => {
            tracing::debug!(id = ?id, reason = ?reason, "cache evicted");
        }
        Ok(_) => {}
        Err(RecvError::Lagged(skipped)) => {
            tracing::warn!(skipped = skipped, "event observer lagged");
        }
        Err(_) => break,
    }
}
```

`Punnu::invalidate(..., InvalidationReason::Manual)` uses the narrow public
reason set and appears on `EventReason::Manual`. The event stream is wider and
can also report `LruEvict`, `TtlExpired`, and `BackendInvalidation` internally.

## Metrics

Observability hooks are opt-in via `PunnuConfig::metrics`:

```rust
struct Metrics;
impl sassi::PunnuMetrics for Metrics {
    fn record_hit(&self, type_name: &'static str, tier: sassi::CacheTier) {
        tracing::debug!("{type_name} hit in {tier:?}");
    }
    fn record_miss(&self, type_name: &'static str) {
        tracing::debug!("{type_name} miss");
    }
    fn record_eviction(&self, type_name: &'static str, reason: sassi::EventReason) {
        tracing::debug!("{type_name} eviction: {reason:?}");
    }
    fn record_backend_error(&self, type_name: &'static str, err: &sassi::BackendError) {
        tracing::warn!("{type_name} backend error: {err}");
    }
    fn record_fetch_latency(&self, type_name: &'static str, duration: std::time::Duration) {
        tracing::debug!("{type_name} fetch latency: {duration:?}");
    }
    fn record_lru_size(&self, type_name: &'static str, size: usize) {
        tracing::debug!("{type_name} size: {size}");
    }
}

let _users = Punnu::<User>::builder()
    .config(PunnuConfig {
        metrics: Some(std::sync::Arc::new(Metrics)),
        ..Default::default()
    })
    .build();
```

Callbacks run synchronously on the operation path, so keep them cheap and avoid
re-entering `Punnu` calls from inside a callback; panic callbacks are trapped and
logged as non-fatal.

## L1/L2 Access Boundaries

- `get` is L1-only.
- `get_async` checks L1 first, then backend, then inserts into L1.
- `get_or_fetch` and `get_or_fetch_many` are canonical-id fetchers and do not
  write through to L2 on success.

`get_or_fetch_many` returns `Vec<Arc<T>>` without preserving input order; it
combines in-memory hits with fetched values for matched IDs.

If you ingest wire payloads, use `sassi::wire` plus `insert_serialized`:

```rust
let payload = sassi::wire::to_vec(&value)?;
let _entry = users.insert_serialized(&payload).await?;
let _value = sassi::wire::from_slice::<User>(&payload)?;
```

Sassi's `serde` feature uses a postcard-backed binary container for value wire.
The header carries Sassi magic bytes, wire major `1`, a kind byte, zero flags,
and `Cacheable::cache_type_name()` before postcard payload bytes. Readers
validate the header before decoding the typed payload, so an incompatible major,
kind, type, or flags is rejected before the payload is ever touched. TTL is a
local pool policy and is not embedded in the wire bytes.

The wire major is independent of the crate semver. It is exposed as
`sassi::wire::WIRE_FORMAT_MAJOR` for diagnostics; future shape changes bump that
major rather than the crate version.

## Punnu Entries Export And Restore

`Punnu::export_entries_postcard()` exports unexpired entries only. It does not
export TTL deadlines, LRU epochs, refresh handles, event listeners, in-flight
fetches, backend stale-read suppression, or runtime/executor state. Export
clones `Arc<T>` handles into one immutable snapshot, sorts by `Cacheable::Id`,
and serializes borrowed `&T` values into a postcard-backed snapshot container.

```rust
let bytes = pool.export_entries_postcard()?;
```

The postcard methods are Sassi's compact first-party snapshot convenience, not
the only way to move cached data. Applications that need human-readable
export, cross-language APIs, spreadsheets, or external contracts should
extract the same projection entries as Rust values and serialize those
app-owned DTOs with `serde_json`, YAML, CSV, protobuf, or another format
appropriate to that contract.

`Punnu::restore_entries_postcard(bytes)` replaces the receiving pool's L1
entries from the snapshot, applies the receiving pool's TTL policy, and emits
normal L1 events after the restored snapshot is visible. Restore is L1-only
and synchronous: it does not write through to L2 and does not publish backend
invalidations.

```rust
let stats = pool.restore_entries_postcard(&bytes)?;
// stats.inserted, stats.updated, stats.removed
```

Restore rejects oversized snapshots (`TooManyEntries`), duplicate ids
(`DuplicateId`), type-name mismatches (`WireFormat`), and snapshots arriving
while a strict backend write is in flight (`BackendWriteInFlight`) before any
L1 mutation. Restored entries receive fresh target-pool `default_ttl` and
fresh LRU epochs because the snapshot does not carry the source pool's TTL
deadlines.

Restore treats the incoming snapshot as authoritative whole-L1 state. It
therefore replaces same-id residents even when the receiving pool is configured
with `OnConflict::Reject`; that conflict policy applies to ordinary inserts,
not to snapshot restore.

The binary kind value for `entries_with_hints` is reserved for a future
operational handoff mode. Hints may include remaining TTL and approximate
recency order. Hints are best-effort and may be ignored by restore. This is
not full internal-state export.

Full internal-state export remains unsupported: active refresh handles,
subscription watermarks/recovery sets, single-flight work, event listeners,
backend stale-read suppression, and runtime/executor state are process-local.
Applications that need gRPC/microservice continuity should send app-level
generations, sync cursors, or event-log positions beside the Punnu entries
snapshot.

## TTL and Size Semantics

TTL is lazily enforced on read:

- `get` returns `None` for expired entries.
- the expired entry may remain in `L1` until a writer path or sweep cleans it.
- no removal event is emitted for this lazy read path.

`Punnu::len()` is a snapshot and can include expired-but-not-yet-collected
entries. Configure `ttl_sweep_interval` when you want resident size and event
timing to track physical cleanup more closely.

## Sassi Orchestrator

`Sassi` owns typed pools by concrete model type and supports cross-type trait
queries registered through `#[sassi::trait_impl]`.

It is not a multi-tenant registry for several pools of the same `T`.
Re-registering a type replaces the previous pool. Use separate `Sassi`
instances, wrapper types, or tenant-qualified ids when the application needs
those boundaries to be distinct.

```rust
use std::sync::Arc;

trait Nameable: Send + Sync {
    fn display_name(&self) -> &str;
}

#[sassi::trait_impl]
impl Nameable for User {
    fn display_name(&self) -> &str {
        &self.name
    }
}

let mut orchestrator = Sassi::new();
orchestrator.register::<User>(Arc::new(user_pool));
orchestrator.register::<Team>(Arc::new(team_pool));

let nameables: Vec<Arc<dyn Nameable>> = orchestrator.all_impl::<dyn Nameable>();
```

`all_impl` is constrained to traits that are `Send + Sync + 'static`; that
bound is surfaced as a compile-time error if missing. Re-registering a concrete
type replaces the prior pool for that `TypeId` in this `Sassi` instance.
