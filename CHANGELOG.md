# Changelog

## Unreleased

## [0.1.0-beta.3] - 2026-05-15

### Added

- Added `JSahibON`, Sassi's portable JSON cache value, with finite `f64`
  storage, postcard-compatible serde, order-insensitive object equality,
  optional `serde_json` bridge support behind `serde-json-bridge`, and local
  JSON field predicates.
- Added `sassi::wire::SassiWire`, `WirePortable`,
  `wire::to_vec_portable`, `wire::from_slice_portable`, and
  `#[cacheable(wire_portable)]` as an opt-in postcard wire portability guard.
  The strict helpers delegate to the existing wire helpers without changing
  wire bytes, header kind, flags, or wire major.

### Notes

- The wire portability guard is an allowlist and diagnostic aid, not a proof of
  serde behavior. Manual marker impls can lie, and existing backend/snapshot
  APIs keep their loose serde bounds.

## [0.1.0-beta.2] - 2026-05-08

### Added

- Added `Punnu::export_entries_postcard()` and
  `Punnu::restore_entries_postcard()` for L1-only entries snapshots. Restore
  is synchronous, applies the receiving pool's `default_ttl`, and rejects
  oversized, duplicate-id, type-mismatched, or strict-backend-in-flight
  snapshots before any L1 mutation.
- Added `PunnuRestoreStats` and `PunnuSnapshotError` (behind the `serde`
  feature) for the new restore path.

### Changed

- Breaking: Sassi value wire changed from JSON v0 to postcard v1. The wire
  bytes now carry a fixed binary header (Sassi magic, little-endian wire
  major `1`, kind byte, flags byte, `Cacheable::cache_type_name()`) followed
  by a postcard-encoded payload.
- Breaking: `wire::to_vec` and `wire::from_slice` now require
  `T: Cacheable + serde` so the binary header can validate
  `Cacheable::cache_type_name()` before the payload is decoded.
- Breaking: `wire::WIRE_FORMAT_MAJOR` changed from a `u64` JSON major `0` to
  a `u16` binary major `1`.
- Breaking: `FileBackend` now writes `.sassi` binary cache records with
  inline expiry tags. Beta.1 `.json` cache files and `.ttl` sidecars are
  ignored on read; operators should treat them as cold misses or clear them
  during upgrade.
- Replaced `WireFormatError::Serde(serde_json::Error)` with structured
  binary-header and codec variants (`VersionMismatch`, `InvalidMagic`,
  `KindMismatch`, `UnsupportedKind`, `UnsupportedFlags`, `TypeNameMismatch`,
  `MalformedHeader`, `Codec`). Postcard's own error type is intentionally
  not part of the public surface.

### Documentation

- Documented the postcard binary wire container (header layout, kind bytes,
  type-name validation, fixed-width integer guidance for portable payloads).
- Documented the shared-L2 upgrade story for adopters carrying beta.1 backend
  data into beta.2 (FileBackend `.sassi`/`.json` extension change, Redis and
  custom-backend keyspace clear/namespace roll).
- Documented the local-snapshot vs shared-backend boundary, including
  service-side Redis pools that should keep L2 mutation on backend APIs and
  frontend/mobile/edge pools that hydrate local L1 from platform storage.
- Reserved the `entries_with_hints` binary kind for a future operational
  handoff mode and documented why full internal-state export remains out of
  scope.

### Notes

- The last commit with JSON v0 wire is
  `b93f334dfcf1be4e3026335598a007bcd700bcce` (`v0.1.0-beta.1`).
- `serde_json` remains in the dependency tree because backend key derivation
  and Redis invalidation control messages still use JSON; only the value
  wire and FileBackend record bodies moved to postcard.

## [0.1.0-beta.1] - 2026-05-07

### Added

- Added `IntoBasicPredicate<T>` so downstream crates can expose provenanced
  predicate wrappers while still feeding Punnu's in-memory evaluator.
- Added `PresentField<T, V>` and `Field<T, Option<V>>::some()` for comparing
  only present optional values without treating `None` as an inner default.
- Added `CacheableFieldsMode::External` for downstream macro crates that own
  their own `Cacheable::Fields` companion type.

### Changed

- Made `PunnuScope::filter_basic` and `MemQ::filter_basic` accept
  `IntoBasicPredicate<T>`.
- Made `BasicPredicate<T>` clone structurally without imposing `T: Clone`.
- Changed case-insensitive string predicates to ASCII-only folding so portable
  in-memory semantics can be mirrored exactly by database emitters.
- Made `retry_delay_for_attempt` internal; retry backoff remains covered by
  crate-local tests without exposing the helper as public API.
- Updated public docs and crate metadata for the `0.1.0-beta.1` release line.
- Documented `sassi-macros` and `sassi-codegen` as support crates; ordinary
  adopters depend on `sassi` and optionally `sassi-cache-redis`.
- Documented Redis `invalidate_all` as best-effort across the delete/publish
  boundary.

### Fixed

- Suppressed local `get_async` L2 rehydration for ids whose best-effort backend
  invalidation failed, preventing stale backend values from being resurrected in
  the same process.
- Restored delta-refresh recovery snapshots and primed subscription membership
  when a panic occurs after recovery query preparation.

## [0.1.0-alpha.2] - 2026-05-03

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

### Fixed

- Aligned release metadata and public documentation links with the reviewed
  release commit rather than the older `v0.1.0-alpha.1` tag.
- Made `MemoryBackend` TTL expiry use Sassi's runtime-aware monotonic clock so
  paused Tokio time drives backend TTL tests the same way it drives L1 TTL
  tests.
- Rejected Redis TTL values that overflow Redis' absolute millisecond window
  instead of silently storing them as persistent values.
- Moved missing-runtime diagnostics for periodic and delta refresh startup to
  the public `Punnu` methods.

### Documentation

- Clarified that `BackendFailureMode::Error` applies to operations that touch
  L2; fetch and refresh helpers apply fetched values to L1 and do not publish
  query membership changes through L2 invalidation.
- Clarified that `FileBackend` uses blocking filesystem calls and is intended
  for development, tests, and simple local persistence rather than production
  request-path load.
- Added adopter guide coverage for events, metrics, custom backends, delta
  handles, direct delta application, wire ingress, TTL cleanup semantics,
  tenant identity boundaries, and runtime guardrails.

### Notes

- Sassi is framework-neutral. Dioxus/full-stack Bardownski work is intentionally
  outside this dependency-light repository.
- Benchmark numbers are same-host regression signals, not portable throughput
  guarantees.
- WASM runtime execution tests are tracked separately from the current compile
  path gate.
