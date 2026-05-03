# Changelog

## [0.1.0-alpha.1] - 2026-05-03

### Added

- `Cacheable` identity trait, `Field<T, V>` accessors, and
  `#[derive(Cacheable)]`.
- `BasicPredicate<T>` algebra with typed field lookups and boolean
  composition.
- `MemQ<T>` in-memory query pipeline for resident values.
- `Punnu<T>` typed pool with immutable L1 snapshots, sampled-LRU, optional TTL,
  events, metrics, explicit invalidation, and conflict policy.
- Lazy fetch helpers: `get_or_fetch` and `get_or_fetch_many`.
- Periodic refresh and watermark-based delta refresh, including tombstones,
  recovery sets, full-refresh policies, and per-subscription single-flight.
- `CacheBackend<T>` with memory and file backends in the core crate.
- `sassi-cache-redis` companion crate with Redis storage and pub/sub
  invalidation.
- Versioned Sassi wire envelope with future-major rejection before payload
  decode.
- `Sassi` orchestrator and `#[sassi::trait_impl]` for cross-type trait queries.
- Native Tokio runtime support and a verified `wasm32-unknown-unknown` compile
  path through `runtime-wasm`.
- Dependency-light `examples/bardownski` TUI showcase.
- Criterion benchmark harness for same-host release baselines.
- Public adopter docs under `docs/`.

### Notes

- Sassi is framework-neutral. Dioxus/full-stack Bardownski work is intentionally
  outside this dependency-light repository.
- Benchmark numbers are same-host regression signals, not portable throughput
  guarantees.
- WASM runtime execution tests are tracked separately from the current compile
  path gate.
