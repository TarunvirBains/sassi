# sassi

Sassi is a typed cache substrate for Rust applications.

It gives you an in-process identity map, query predicates, refresh loops, delta
sync, and optional backing storage without asking you to adopt an ORM, a web
framework, or a database client. The goal is to make cached application data
feel like ordinary Rust data: typed, testable, portable across native and WASM
targets, and explicit about the places where cache correctness usually gets
fuzzy.

Sassi is being built alongside [djogi](https://github.com/TarunvirBains/djogi),
but it is not djogi-specific. A service, desktop app, Dioxus frontend, worker,
or library can use Sassi as the shared caching layer under its own data model.

## Why it exists

Rust has strong tools for databases, async services, and frontend state, but
there is still a gap between "query the source of truth again" and "keep a
safe, typed local view of the data I already fetched." Most projects fill that
gap with ad hoc maps, request-scoped caches, stale globals, or framework-local
state.

Sassi tries to make that middle layer boring:

- cache entries by their real typed identity
- keep reads cheap with immutable L1 snapshots
- express reusable predicates once
- run the same data model on backend and WASM frontend builds
- refresh whole sets or delta streams without hiding query state inside the
  cache
- plug in a backend cache when a process-local L1 is not enough

## Core pieces

- `Punnu<T>` is the typed pool. It stores `Arc<T>` values by `Cacheable::Id`,
  uses bounded sampled-LRU eviction, supports optional TTL, and can publish
  cache events.
- `Cacheable` is the trait that tells Sassi how to identify a value. The derive
  macro can also mark a monotonic watermark field for delta sync.
- `BasicPredicate<T>` is a serializable predicate algebra for filters that can
  be projected to a data layer and replayed in memory.
- `MemQ<T>` adds in-memory-only predicates for closures and trait-based checks.
- `start_periodic_refresh` runs simple polling refreshes.
- `start_delta_refresh` runs watermark-based subscriptions with per-subscription
  watermarks, single-flight updates, eviction recovery, and periodic full
  refresh policies.
- `CacheBackend` lets Sassi use Redis, file, memory, or other backend stores
  behind the same cache trait.
- `Sassi` is the process orchestrator for cross-type pools and trait queries.

## Refresh Model

Use `start_periodic_refresh` when a fetcher returns a complete or partial list
on a timer and a simple upsert or replace policy is enough.

Use `start_delta_refresh` when the source can answer "what changed since this
watermark?" Delta refreshers own their own watermark, recovery set, and
single-flight slot, so several query-shaped subscriptions can feed the same
`Punnu<T>` without advancing each other's cursors.

A single `Punnu<T>` stores the resident union for that type. It does not store
hidden query membership. Query-specific reads should use `scope()` predicates,
and query-specific refreshers should not use `RefreshMode::Replace` unless they
are authoritative for the whole resident set.

Refresh subscriptions should be driven by data-layer-projectable filters. In
Sassi terms, that means `BasicPredicate` constraints. `MemQ` closures are for
in-memory reads after data has reached the pool; they cannot be replayed safely
by a backend fetcher.

Delta tombstones mean true deletes from the identity map. If a row merely stops
matching a query, return the updated row and let predicates stop selecting it.
For soft deletes, keep the deleted marker on the value and opt into visibility
at the data layer when a fetcher needs to recover or migrate those rows.

## Watermarks

Delta sync requires a monotonic watermark. With `#[derive(Cacheable)]`, use:

```rust
#[derive(sassi::Cacheable)]
#[cacheable(watermark_field = "updated_at")]
struct User {
    id: i64,
    updated_at: i64,
}
```

Sassi ships std-only watermark impls for integers, `SystemTime`, and small
tuples. Enable `watermark-time` for `time` types and `watermark-chrono` for
`chrono` types.

Fetchers should query with an inclusive `>= since` boundary. Returning the
boundary row again is correct; Sassi deduplicates by identity and never rolls
the subscription watermark backward.

## Auth And Tenancy

Sassi does not infer tenant, auth, pagination, or row-level-security boundaries
from cached values. Put those dimensions in the type, in the id, in a wrapper
key, or in the fetcher/subscription that owns the query.

For backend/RLS work, make the fetcher own a `'static` substrate such as a pool,
client, or factory, then construct the per-call data-layer context inside
`fetch_delta`. Do not share a request-scoped transaction or borrowed auth guard
with a background refresh task.

`PunnuConfig::namespace` is for backend keyspace separation. It does not isolate
the in-process L1 map.

## Status

Sassi is pre-v0.1.0. The public shape is close, but this repository is still in
active implementation and review before the first release cut.

The current implementation targets native Rust and `wasm32-unknown-unknown`.
WASM compile coverage is in place; broader per-test WASM execution tracks
[issue #3](https://github.com/TarunvirBains/sassi/issues/3).

## Workspace

```text
sassi/          # library crate
sassi-codegen/  # shared code generation used by sassi and djogi macros
sassi-macros/   # proc macros
```

## Naming

The names come from the Punjabi story of Sassi and Punnu. The cache type is
`Punnu` because it is the thing Sassi keeps reaching for. That is the whole
joke; no acronym required.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
