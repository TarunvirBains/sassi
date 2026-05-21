# Sassi

[![Crates.io](https://img.shields.io/crates/v/sassi.svg)](https://crates.io/crates/sassi)
[![Docs.rs](https://docs.rs/sassi/badge.svg)](https://docs.rs/sassi)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Sassi is a typed cache substrate for Rust applications with composable predicate algebra and cross-runtime trait queries. It is built for when cached Rust data stops being just a key-value lookup and starts becoming typed local application state.

It provides an in-memory pool (`Punnu<T>`) that lets you look up domain objects by identity, refresh them, and query a local view using composable predicates (`BasicPredicate<T>`, `MemQ<T>`)—without tying that view to an ORM, a web framework, or a database client. Whether you are building a native service, a desktop worker, or a `wasm32-unknown-unknown` application, Sassi gives your intermediate cache layer a shared, typed shape.

> Named after Sassi and Punnu, the central figures of a classic Punjabi folk
> tale.

The name is meant in that literary context. If it is unfamiliar, the source
material is worth exploring on its own terms; this project borrows from that
tradition, not from an acronym or an invented technical metaphor. Any similarity
to unrelated software, tools, or projects is coincidental. Sassi is independent
and unaffiliated.

## Why Sassi Exists

Rust has excellent key-value caches (`moka`, `cached`) and flexible ways to query collections, but many applications need typed cached state that can be queried without becoming a full database.

As applications grow, they often hand-roll a layer between the source of truth and local readers: identity maps, predicate helpers, refresh tasks, and eviction policies. Sassi gives that intermediate layer a shared, typed shape. It is a cache substrate designed for the space between simple memoization and durable storage: storing values by typed identity, reading through explicit predicates, and keeping cache policy visible in Rust types.

## Quick Example

First add the dependencies, then cache typed users by id and query active
adults. This example is also available as a CI-verified file at
[`sassi/examples/quick_tour.rs`](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/sassi/examples/quick_tour.rs);
the three surfaces (example file, crate-level rustdoc, this README block) mirror
the same body.

```toml
# Cargo.toml
[dependencies]
sassi = "0.1.0-beta.4"
tokio = { version = "1.52", features = ["macros", "rt"] }
```

```rust
use sassi::{Cacheable, MemQ, Punnu};

#[derive(sassi::Cacheable)]
struct User {
    id: i64,
    age: u32,
    is_active: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // 1. Build the in-memory pool and populate it.
    let users = Punnu::<User>::builder().build();
    users
        .insert(User { id: 1, age: 32, is_active: true })
        .await
        .unwrap();

    // 2. Query local state with composable predicates.
    let adults = users
        .scope(vec![MemQ::filter_basic(
            User::fields().age.gte(18) & User::fields().is_active.eq(true),
        )])
        .take(10)
        .collect();
    assert_eq!(adults.len(), 1);
}
```

The `#[derive(Cacheable)]` macro requires a field literally named `id`. Types
whose identifier uses a different name (e.g. `user_id`) need a hand-written
`Cacheable` impl.

## Concepts

- **`Cacheable`**: Tells Sassi how to identify a value, exposing canonical identities (`Id`) and searchable fields.
- **`Punnu<T>`**: The in-process typed object pool. It stores values by identity and publishes new immutable snapshots after writes.
- **`BasicPredicate<T>`**: A shared predicate algebra (using `&`, `|`, `^`, `!`) that is walkable. This allows both Sassi to replay it in memory and your fetchers to inspect it.
- **`MemQ<T>`**: For local work after data has reached the pool: filtering, sorting, taking, mapping, unique-ing, grouping, and folding.

## Cache Lifecycle Features

- **Fetch-on-miss and coalescing**: `get_or_fetch` collapses concurrent fetches for the same id into a single execution. `get_or_fetch_many` deduplicates ids within a single batch call; concurrent batch calls for the same ids are not presently cross-coalesced.
- **Bounded Eviction**: Sampled-LRU eviction and optional TTL keep your memory footprint bounded. TTL cleanup is lazy by default — expired entries are observed as misses by `get` and reclaimed under capacity pressure; configure `ttl_sweep_interval` to opt into a background sweep task.
- **Refresh**: Built-in mechanisms for periodic polling and watermark-based delta sync drive background updates. Eviction recovery (re-fetching entries that LRU pressure dropped) is opt-in via `DeltaRefreshHandle::with_eviction_recovery(true)`.
- **Events**: Subscribe to streams of inserts, updates, and deletes to trigger application UI renders or downstream side effects. Events are best-effort observability with a lossy contract — slow subscribers and missing subscribers drop events — not a durable log; build durable side-effect pipelines on the source of truth instead.

When using an L2 backend (Redis, FileBackend), set
`#[cacheable(type_name = "stable.name")]` on cached types — the default
`Cacheable::cache_type_name()` is tied to the Rust type path, so renaming a
module invalidates the L2 keyspace. See
[Backends And Runtimes](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/backends-and-runtimes.md)
for full L2 keyspace mechanics. Configuration details for TTL durations,
eviction thresholds, and refresh intervals also live in that document.

## When to Use Sassi

**Use Sassi when:**
- Your cached data is becoming typed local application state.
- You need to query your cached data by attributes (using composable predicates) alongside identity lookups.
- You need structured cache lifecycles: lazy fetch-on-miss coalescing, TTL/LRU eviction, and event streams.
- You need your cache layer to expose the same API surface across native targets and `wasm32-unknown-unknown` (runtime-specific code paths remain internal).

**Do not use Sassi when:**
- You only need simple exact-key lookup (use `moka` or `cached`).
- Your data is small, transient, and local to a single function (use `HashMap` or `Vec::iter().filter()`).
- You need durable transactions, secondary indexes, query planning, or relational joins (use a real database).
- You are looking for a full LINQ clone or a database ORM.

## Comparisons

- **`HashMap` / `Vec::iter().filter()`**: Great for small, transient data. However, they do not provide cache eviction policies (LRU/TTL), background refresh, or cross-request single-flight fetch coalescing.
- **`moka`**: An excellent, high-performance concurrent cache for exact-key lookups. It is optimized for hit-rate and throughput for key-value data. Use `moka` when you only need `get(key)`. Sassi focuses on typed object pools where data is queried by predicates alongside key lookups.
- **`cached`**: A fantastic tool for simple memoization and function-level caching. Sassi operates at the domain-model level rather than the function-return level, maintaining a queryable local view of the data.
- **SQL/LINQ-style collection query builders**: Great for general-purpose querying over Rust collections. Sassi is not a general-purpose query builder; it is a cache pool that happens to support basic predicate filtering, explicitly avoiding joins and complex relational algebra.
- **Databases (SQLite, PostgreSQL, etc.)**: Provide durable storage, ACID transactions, secondary indexes, and query planning. Sassi provides none of these. Sassi is an in-memory cache substrate for fast, local reads; predicate queries in Sassi are currently evaluated as in-memory scans.

## What Sassi Is Not

- **Not a database:** Sassi does not provide ACID transactions, query planning, or secondary indexes. The optional `FileBackend` writes cache values to disk for development and simple local persistence, but L2 backends in Sassi are cache substrate, not a system of record.
- **Not a full ORM:** Sassi maps cache identities to objects, but it does not map database schemas or track foreign key relations.
- **Not a full LINQ clone:** Predicates are limited to basic logical composition (`&`, `|`, `^`, `!`) and do not support arbitrary joins or complex relational projection.
- **Not a replacement for simple caches:** If you only need `get(key)`, Sassi introduces unnecessary overhead compared to specialized K/V caches.
- **Not a magic index/query planner:** Predicate queries in Sassi are currently evaluated as in-memory scans. There is no query planner or secondary indexing.

## Status

Sassi's current public beta is `0.1.0-beta.4`. The core API, Redis companion, Bardownski TUI example, benchmark harness, and adopter docs are in place, with the caution that beta APIs can still move when integration work finds correctness or ergonomics gaps.

## Workspace

```text
sassi/              # library crate
sassi-codegen/      # support crate for macro/codegen integrations
sassi-macros/       # support proc-macro crate re-exported by sassi
sassi-cache-redis/  # Redis CacheBackend companion crate
examples/bardownski/ # dependency-light TUI showcase
xtask/              # internal build tooling (not published)
```

Most adopters add only `sassi` to `Cargo.toml`, plus `sassi-cache-redis` when
Redis L2 support is needed. `sassi-macros` and `sassi-codegen` are published
support crates for Sassi's derive macros and downstream macro integrations; they
are not part of the ordinary application dependency story.

Authors writing their own proc-macro crates against `sassi-codegen` must depend
on `proc-macro-crate` at a version compatible with the one `sassi-codegen` pins
(see `sassi-codegen/Cargo.toml`). `sassi_codegen::resolve_sassi_path` accepts a
`proc_macro_crate::FoundCrate` parameter in its public signature, so the two
crates must agree on the `FoundCrate` type identity.

## Documentation

- [Getting Started](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/getting-started.md)
- [Concepts](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/concepts.md)
- [Query And Refresh Boundaries](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/query-refresh-boundaries.md)
- [Backends And Runtimes](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/backends-and-runtimes.md)
- [Dependency Footprint](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/dependency-footprint.md)
  — transitive dep graph per feature combination, for adopters auditing
  binary size or supply-chain surface.
- [Advanced Guide](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/advanced-guide.md)
  — predicate walk surface, scope chaining, `MemQ` terminals, `#[trait_impl]`
  registry, delta refresh handle operations, snapshot/restore modes, and
  custom-backend implementer notes.
- [Release Readiness](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/docs/release-readiness.md)
- [Bardownski TUI Showcase](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/examples/bardownski/README.md)
- [Benchmarks](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/sassi/benches/README.md)
- [Contributing](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/CONTRIBUTING.md)
  (pre-commit guidance, the sensitive-info guard, and the public-text
  placeholder conventions)
- [Changelog](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/CHANGELOG.md)

## License

Dual-licensed under
[MIT](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/LICENSE-MIT)
or
[Apache-2.0](https://github.com/TarunvirBains/sassi/blob/v0.1.0-beta.4/LICENSE-APACHE).
