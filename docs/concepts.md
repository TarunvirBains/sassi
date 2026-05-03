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
`BackendFailureMode::Error` when L2 is part of the correctness boundary.

## Sassi Orchestrator

`Sassi` owns typed pools by concrete model type and supports cross-type trait
queries registered through `#[sassi::trait_impl]`.

It is not a multi-tenant registry for several pools of the same `T`.
Re-registering a type replaces the previous pool. Use separate `Sassi`
instances, wrapper types, or tenant-qualified ids when the application needs
those boundaries to be distinct.
