# Changelog

## Unreleased

### Added

- `PunnuScope::filter_impl::<Trait>()` — single-type, compile-time trait-narrowed scope filter. Gated on new `TraitImpl<Trait>` marker emitted by `#[sassi::trait_impl]`.
- `TraitImpl<Trait>` public marker trait (zero runtime cost).

### Changed

- `cargo-lihaaf` dev-tool pin bumped `0.1.0-beta.9` → `0.1.0-beta.10` in
  `.github/workflows/ci.yml`, `sassi-macros/Cargo.toml`, and
  `docs/release-readiness.md`. Local fixture canary on `sassi-macros`
  (30/30 OK) confirmed before the pin change.

## [0.1.0-beta.4] - 2026-05-18

### Added

- Added `sassi_codegen::resolve_sassi_path`, a stable public path-resolution
  helper extracted from `sassi-macros`. Downstream macro crates that consume
  `sassi-codegen` can now share the same `proc-macro-crate` resolution logic
  without duplicating the `FoundCrate::Itself` vs `FoundCrate::Name` match.
  The function is regression-gated by a dedicated 52-line mutation-tested unit
  test (`sassi-codegen/tests/sassi_path.rs`) that asserts the emitted token
  strings byte-for-byte for both `FoundCrate` arms.
- Added `proc-macro-crate` as a public dependency of `sassi-codegen`.
  `resolve_sassi_path` accepts a `proc_macro_crate::FoundCrate` parameter in
  its public signature, so downstream macro crates that call it must also
  depend on `proc-macro-crate` at a compatible version.

### Changed

- Changed the `sassi-macros` macro expansions to emit the absolute path
  `::sassi` instead of the `crate` keyword for the `FoundCrate::Itself` arm.
  This affects both `#[derive(Cacheable)]` and `#[sassi::trait_impl]`, which
  share the new `sassi_codegen::resolve_sassi_path` helper. The earlier
  `crate` form failed inside `sassi`'s own integration tests and doctests
  because, per standard proc-macro hygiene, `crate` in the expanded token
  stream resolves to the **caller's** crate (the crate being compiled that
  invoked the macro) — in those tests, the doctest harness or
  integration-test binary, neither of which defines `Cacheable`. The
  absolute path `::sassi` resolves correctly regardless of the calling
  context.

### Fixed

- Gated the `unresolved_snapshot_panics_in_debug` test in
  `sassi/src/punnu/recovery.rs` on `cfg(debug_assertions)`. The
  `#[should_panic]` assertion targets a `debug_assert!`-driven panic that
  cannot fire under `cargo test --release`, so the release-mode test suite
  no longer reports a spurious failure for that case.

### Documentation

- Rewrote the crate landing README around accurate adopter framing: a
  derive-based Quick Example with a visible async runtime context, an
  honest description of `get_or_fetch_many` coalescing semantics, the
  opt-in nature of eviction recovery, the lossy-events caveat, a narrower
  "Not a database" claim, a `cache_type_name` rename gotcha callout near
  the L2 mention, and removal of the phantom `rust-queries-builder`
  comparison. The workspace-layout block now lists `xtask/` (annotated
  "internal build tooling, not published"), and the documentation index
  links the existing `docs/dependency-footprint.md` page.
- Added `[package.metadata.docs.rs] all-features = true` to `sassi/Cargo.toml`
  so docs.rs renders the full feature surface (`runtime-tokio`,
  `runtime-wasm`, `serde`, `serde-json-bridge`, watermark types) on a
  single docs page.
- Rewrote the `sassi/src/lib.rs` crate-level rustdoc. The docs.rs landing
  page now leads with the "typed cache substrate" framing, includes a
  derive-based Quick Tour example wrapped in `async fn run()`, and
  documents the `Cacheable` derive's requirement that the struct carry a
  field literally named `id`.
- Refreshed the `sassi` crate metadata to match the README's "cache
  substrate" framing: rewrote `description` from "Typed in-memory pool
  with composable predicate algebra and cross-runtime trait queries." to
  "Typed cache substrate for Rust applications with composable predicate
  algebra and cross-runtime trait queries."; updated `keywords` (added
  `in-memory` and `wasm`, removed `lru` and `rust`); added `wasm` to
  `categories` for discoverability under WebAssembly searches on
  crates.io.
- Added `sassi/examples/quick_tour.rs`, a CI-verified example file that
  mirrors the lib.rs Quick Tour doctest and the README Quick Example
  body. The workspace `cargo clippy --all-targets` gate compiles it so
  the three-place mirror cannot drift in code, only in prose.
- Added a "Documentation Invariants" subsection to
  `docs/release-readiness.md` documenting the three-place Quick Tour
  mirror contract (`sassi/examples/quick_tour.rs`, the `sassi/src/lib.rs`
  crate-level rustdoc, and the `README.md` Quick Example block) and which
  CI gates catch drift in each.
- Added a "`sassi-codegen` Public Dependency Footprint" section to
  `docs/dependency-footprint.md` documenting the semver-relevant public
  dependencies (`proc-macro-crate`, `proc-macro2`, `syn`) that downstream
  macro authors must keep in version-range agreement. (`quote` is a
  `sassi-codegen` dependency but private to macro bodies, not a public
  semver constraint for downstream crates.)
- Rewrote the `docs/concepts.md` snapshot-mode reference (around the
  internal-state hints kind) to use the adopter-visible
  `SnapshotMode::EntriesOnly` / `SnapshotMode::WithInternalState`
  spelling instead of the private `KIND_PUNNU_ENTRIES` /
  `KIND_PUNNU_ENTRIES_WITH_HINTS` constant names.
- Expanded the lihaaf gate description in `docs/release-readiness.md` so
  it explicitly mentions `#[sassi::trait_impl]` attribute expansions
  alongside `#[derive(Cacheable)]` errors and `MonotonicWatermark`
  trait-bound rejections.
- Updated the eviction-recovery opt-in mention in `README.md` to
  reference the public `DeltaRefreshHandle::with_eviction_recovery(true)`
  API rather than the internal `eviction_recovery_enabled` field name.
- Documented the `proc-macro-crate` semver public-dependency constraint
  for downstream macro authors in both `README.md`'s workspace section
  and `docs/dependency-footprint.md`.
- Dropped the cargo-cult `Clone` derive from the Quick Example /
  Quick Tour `User` struct in `README.md` and `sassi/src/lib.rs`;
  `Cacheable` does not bound `T: Clone`.
- Wrapped the README Quick Example async body in
  `#[tokio::main(flavor = "current_thread")]` so the first-screen sample
  matches the runnable `sassi/examples/quick_tour.rs` file. The block now
  also lists the minimal `Cargo.toml` dependencies adopters need before
  copy-pasting.
- Changed the rustdoc code fence for the `#[sassi::trait_impl]` example
  block in `sassi-macros/src/lib.rs:52` from ` ```ignore ` to ` ```text `.
  The example is illustrative-only (it cannot compile in the proc-macro
  crate's own doctest sandbox), and the new fence removes the spurious
  "1 ignored doctest" line from `cargo test --doc` output while keeping
  the snippet visible on docs.rs.
- Removed lingering `djogi` sibling-project mentions from published
  rustdoc across the workspace's library and macro crates:
  `sassi/src/cacheable.rs`, `sassi/src/predicate/field_predicate.rs`,
  `sassi/src/predicate/mod.rs`, `sassi-macros/src/lib.rs`,
  `sassi-codegen/src/lib.rs`, and `sassi-codegen/src/fields_struct.rs`.
  Eleven instances total — holdovers from earlier beta lines that still
  rendered on docs.rs for the published crates. Sibling-repo context now
  lives only in maintainer-only docs (`CLAUDE.md`, `CONTRIBUTING.md`),
  not in the published API surface. The Sassi crates remain
  framework-neutral and the no-djogi-pressure gate (`CLAUDE.md`) is now
  honored across all published surfaces.

### Notes

- v0.1.0-beta.4 is a polish-only release on top of v0.1.0-beta.3. There is
  no change to the cache, predicate, refresh, backend, or snapshot
  surface, and no change to the postcard wire bytes. Adopters on beta.3
  do not need to migrate; the rebuild is for the codegen path-resolution
  fix, the release-mode test gate, the docs.rs metadata, and the README
  rewrite.
- `sassi-cache-redis/README.md` and the adopter-guide pages under
  `docs/` received mechanical `0.1.0-beta.3` → `0.1.0-beta.4` version-pin
  substitutions. The `docs/README.md` tagline was updated from "part of
  production behavior" to "typed local application state" to align with
  the new lead-doc framing. Substantive doc content changes (new
  sections, rewrites, public-API renamings in prose) are listed under
  the **Documentation** section above.
- `cargo-lihaaf` dev-tool pin bumped `0.1.0-beta.3` → `0.1.0-beta.9` in
  the CI workflow and contributor docs. No impact on published-crate
  adopters; relevant only to contributors running macro fixtures locally.

## [0.1.0-beta.3] - 2026-05-16

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
- Added a repository sensitive-info guard (`cargo xtask sensitive-info`) and
  GitHub workflow for redacted scanning of public issue, pull request, and
  review text.

### Changed

- Replaced the proc-macro compile-fixture gate with `cargo lihaaf` fixtures
  under `sassi-macros/tests/lihaaf/`; the pinned Lihaaf beta is now part of
  the release verification path.

### Documentation

- Expanded the user guide around portable JSON fields, key/value JSON
  predicates, the `serde-json-bridge` feature, and `#[cacheable(wire_portable)]`
  usage.
- Updated release-readiness guidance with the current CI surface, sensitive
  information guard, dependency recency check, and publish/tag checklist.

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
