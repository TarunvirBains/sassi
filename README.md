# sassi

[![Crates.io](https://img.shields.io/crates/v/sassi.svg)](https://crates.io/crates/sassi)
[![Docs.rs](https://docs.rs/sassi/badge.svg)](https://docs.rs/sassi)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

> Named after Sassi and Punnu, the central figures of a classic Punjabi folk
> tale.

Sassi is a typed cache substrate for Rust applications.

It helps you keep a local, typed view of data you have already fetched without
tying that view to an ORM, a web framework, or a database client. A service,
worker, desktop app, library, or WASM build can use the same data model and the
same cache contracts.

The name is meant in that literary context. If it is unfamiliar, the source
material is worth exploring on its own terms; this project borrows from that
tradition, not from an acronym or an invented technical metaphor. Any similarity
to unrelated software, tools, or projects is coincidental. Sassi is independent
and unaffiliated.

## Why Sassi Exists

Many Rust applications grow a layer between the source of truth and the code
that reads from it: identity maps, predicate helpers, refresh tasks, backend
invalidation, and small caches around expensive calls. Those layers often start
simple. The hard parts tend to arrive gradually: typed identity, freshness,
eviction, query boundaries, runtime portability, and visibility into what the
cache is doing.

Sassi gives that layer a shared shape. It is not trying to replace a database,
an ORM, or an application state framework. It focuses on the cache substrate
beneath them: storing values by typed identity, reading through explicit
predicates, refreshing from fetchers, and keeping cache policy visible in Rust
types.

## What It Provides

- Typed identity maps through `Punnu<T>` and `Cacheable`
- Cheap in-process reads from immutable L1 snapshots
- `BasicPredicate<T>` for constraints a fetcher can inspect and replay
- `MemQ<T>` for in-memory closure and trait-based querying
- Lazy fetch-on-miss with `get_or_fetch` and `get_or_fetch_many`
- Periodic refresh and watermark-based delta refresh
- Bounded sampled-LRU eviction, optional TTL, events, and metrics
- An L2 `CacheBackend` boundary with memory and file backends in the core crate
- Redis support in the `sassi-cache-redis` companion crate
- A `Sassi` orchestrator for typed pools and cross-type trait queries
- Native `tokio` support and a `wasm32-unknown-unknown` compile path

## Core Shape

`Punnu<T>` is the in-process pool. It stores `Arc<T>` values by
`Cacheable::Id`, publishes new immutable snapshots after writes, and lets reads
compose explicit predicates over the resident data.

`Cacheable` tells Sassi how to identify a value. The derive macro can also mark
a monotonic watermark field for delta sync:

```rust
#[derive(sassi::Cacheable)]
#[cacheable(watermark_field = "updated_at")]
struct User {
    id: i64,
    updated_at: i64,
}
```

For durable or shared L2 backends, give the type an application-owned stable
name with `#[cacheable(type_name = "myapp.User")]`. Treat that name as part of
the backend schema: it should be unique inside a namespace and reused only for
wire-compatible payloads keyed by the same ids.

`BasicPredicate<T>` is the shared predicate algebra. It is walkable and
data-layer-projectable, so a fetcher can understand the same constraints that
Sassi can replay in memory. It is not a serde-serializable wire format in
v0.1.0.

`MemQ<T>` is for local work after data has reached the pool: closure filters,
map, sort, take, unique, group, partition, and fold.

`start_periodic_refresh` handles simple polling. `start_delta_refresh` handles
watermark-based subscriptions with per-subscription cursors, single-flight
updates, eviction recovery, and periodic full-refresh policies.

`Sassi` is the process-level orchestrator for typed pools and cross-type trait
queries registered with `#[sassi::trait_impl]`.

## Design Notes For Adopters

A `Punnu<T>` is a resident union for a type, not a stored result set for one
query. Several subscriptions can feed the same pool. They share the identity
map, but each subscription owns its fetcher, filter, watermark, and recovery
state.

That tradeoff is intentional. Shared identity keeps memory use and cache
coherence manageable, while per-query inclusion stays explicit at read and
refresh boundaries. If a row no longer matches one query, return the updated row
and let predicates stop selecting it. Use tombstones for true deletes from the
identity map. Use `RefreshMode::Replace` only when the fetcher is authoritative
for the whole resident set.

Sassi also does not infer tenant, auth, pagination, or row-level-security rules
from cached values. Put those boundaries in the type, in the id, in a wrapper
key, or in the fetcher/subscription that owns the query. `PunnuConfig::namespace`
separates backend keyspaces; it does not isolate the in-process L1 map.

The core crate targets native Rust and `wasm32-unknown-unknown`. Native
background work uses the `runtime-tokio` feature. WASM background work uses the
`runtime-wasm` feature, backed by `wasm-bindgen-futures` and `gloo-timers`.

This repository verifies the WASM compile path *and* runs the
`runtime-wasm` integration test suite under node via `wasm-bindgen-test`,
covering spawn, sleep, TTL sweep, and periodic refresh on the wasm executor.
Sassi has no Dioxus-specific API in this repository; a Dioxus app is one
possible consumer of the WASM build, not a separately certified integration
here.

## Status

Sassi's current public beta is `0.1.0-beta.2`. The core API,
Redis companion, Bardownski TUI example, benchmark harness, and adopter docs are
in place, with the caution that beta APIs can still move when integration work
finds correctness or ergonomics gaps.

The current beta minimum supported Rust version is 1.95 (set in
`[workspace.package].rust-version`).

Adopter feedback is welcome. If Sassi looks useful but a workflow is unclear,
an API feels awkward, or an integration path is missing, please
[open a GitHub issue](https://github.com/TarunvirBains/sassi/issues). Early
adopter friction is useful signal for the v0.1.x surface.

The current release path is focused on the library crate, the Redis companion,
and the dependency-light `bardownski` TUI example. A heavier Dioxus/full-stack
Bardownski implementation is planned outside this repository.

## Workspace

```text
sassi/              # library crate
sassi-codegen/      # support crate for macro/codegen integrations
sassi-macros/       # support proc-macro crate re-exported by sassi
sassi-cache-redis/  # Redis CacheBackend companion crate
examples/bardownski/ # dependency-light TUI showcase
```

Most adopters add only `sassi` to `Cargo.toml`, plus `sassi-cache-redis` when
Redis L2 support is needed. `sassi-macros` and `sassi-codegen` are published
support crates for Sassi's derive macros and downstream macro integrations; they
are not part of the ordinary application dependency story.

## Documentation

- [Getting Started](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/getting-started.md)
- [Concepts](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/concepts.md)
- [Query And Refresh Boundaries](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/query-refresh-boundaries.md)
- [Backends And Runtimes](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/backends-and-runtimes.md)
- [Advanced Guide](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/advanced-guide.md)
  — predicate walk surface, scope chaining, `MemQ` terminals, `#[trait_impl]`
  registry, delta refresh handle operations, snapshot/restore modes, and
  custom-backend implementer notes.
- [Release Readiness](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/docs/release-readiness.md)
- [Bardownski TUI Showcase](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/examples/bardownski/README.md)
- [Benchmarks](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/sassi/benches/README.md)
- [Changelog](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/CHANGELOG.md)

## License

Dual-licensed under
[MIT](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/LICENSE-MIT)
or
[Apache-2.0](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.2/LICENSE-APACHE).
