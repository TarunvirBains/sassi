# Release Readiness

Sassi v0.1.0-beta.2 is meant to be usable by capable Rust adopters who are
willing to work with an early but candidate API surface. The goal is not to
claim that every future integration is done; it is to make the current
contracts, tradeoffs, and verification expectations visible before publish.

## v0.1.0-beta.2 Scope

In scope for the beta:

- `Cacheable` and `#[derive(Cacheable)]` for typed identity.
- `Punnu<T>` as an in-process resident union identity map.
- `BasicPredicate<T>` and `MemQ<T>` for explicit read scopes.
- `get`, `insert`, `get_or_fetch`, and `get_or_fetch_many` for canonical id
  workflows.
- TTL, sampled-LRU, events, metrics, conflict policy, and explicit
  invalidation.
- Periodic refresh and delta refresh with monotonic watermarks, inclusive
  `>= since` boundaries, identity deduplication, and tombstones for true
  deletes.
- Optional L2 `CacheBackend` support with memory/file in core and Redis in the
  companion crate.
- Postcard-backed binary value wire (`sassi::wire`) with a fixed binary
  header that validates wire major, kind, flags, and
  `Cacheable::cache_type_name()` before the postcard payload is decoded.
- `FileBackend` `.sassi` binary records that publish value and inline expiry
  with one atomic file rename. Beta.1 `.json` cache files are ignored.
- `Punnu::export_entries_postcard` and `Punnu::restore_entries_postcard` for
  L1-only entries snapshots. Restore is synchronous, applies the receiving
  pool's TTL policy, and rejects oversized, duplicate-id, type-mismatched, or
  strict-backend-in-flight snapshots before any L1 mutation.
- A documented shared-L2 upgrade note for adopters carrying beta.1 backend
  data into beta.2.
- Native `runtime-tokio` and a verified `wasm32-unknown-unknown` build *and*
  test path with `runtime-wasm`. The `wasm-target` CI job builds the explicit
  feature matrix and runs the `runtime-wasm` integration suite under
  `wasm-bindgen-test` via `wasm-bindgen-test-runner`, exercising spawn,
  sleep, TTL sweep, periodic refresh, and the postcard wire round-trip.
- Whole-Punnu snapshot wrapper (`Punnu::snapshot_postcard` /
  `Punnu::restore_postcard`) with `SnapshotMode::EntriesOnly` (default,
  byte-compatible with the existing `export_entries_postcard`) and an
  opt-in `SnapshotMode::WithInternalState` mode that preserves remaining TTLs
  and relative sampled-LRU recency. Internal-state hints carry their own
  envelope version independent from the wire major.
- Saturating sampled-LRU access clock at `u64::MAX`. The clock cannot wrap and
  invert eviction priority on long-lived processes; saturation degrades to
  random sampling rather than treating fresh entries as old.
- `serde_json` is no longer part of Sassi's `serde` feature; Sassi proper's
  generic backend storage-key helper uses postcard. The Redis companion crate
  retains JSON for Redis id-to-key encoding and pub/sub invalidation messages.
- `Sassi` orchestration for typed pools and cross-type trait queries.
- The dependency-light
  [Bardownski TUI showcase](../examples/bardownski/README.md) in this
  repository.
- Criterion
  [benchmark baselines](../sassi/benches/README.md) for same-host regression
  tracking of public cache surfaces, including postcard wire round-trips.

## Out Of Scope For This Beta

These are intentionally not release claims for v0.1.0-beta.2:

- Full downstream data-layer integration examples.
- The Bardownski Dioxus/full-stack implementation.
- A custom public executor API.
- Automatic tenant, auth, or row-level-security inference from cached values.
- A serde-encoded predicate wire protocol.
- Refresh-handle state preservation across snapshot boundaries.
  `Punnu::snapshot_postcard` (both modes) does not serialize active refresh
  handles, subscription watermarks/recovery sets, single-flight work, event
  listeners, backend stale-read suppression, or runtime/executor state.
  Applications re-attach refresh handles after restore and resume from a
  consumer-persisted watermark.
- A backend-seeding restore. `restore_entries_postcard` and `restore_postcard`
  are L1-only; a future backend-seeding restore, if needed, would be a
  separate async API.
- Certified framework adapters.
- Automatic cross-process coherence for Redis `put`/`insert` writes without explicit
  invalidation publication.

Those deferrals are not dismissals. They are places where Sassi needs real
integration pressure before it should freeze an abstraction.

## Issue Invitations

Please open a GitHub issue when an adopter path is unclear. Useful categories
include:

- API ergonomics: a type, bound, or method makes correct use awkward.
- Documentation gaps: a concept is understandable only after reading source.
- Query boundaries: tenant, auth, pagination, or refresh behavior is hard to
  model safely.
- Runtime gaps: native, WASM, or framework integration needs a clearer path.
- Backend behavior: Redis, file, memory, or custom backend semantics need more
  examples or sharper contracts.
- Benchmarks: a release benchmark should cover a workload you actually expect
  to run.

## Verification Before Publish

Before publishing a beta, run the commands below from the repository root and
record the results in the release notes or publish checklist:

```bash
cargo test --workspace --locked
cargo test --workspace --all-features --locked
cargo test -p sassi --no-default-features --locked
cargo test -p sassi --no-default-features --features watermark-time --locked
cargo test -p sassi --no-default-features --features watermark-chrono --locked
cargo lihaaf --manifest-path sassi-macros/Cargo.toml
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked
cargo check -p sassi --target wasm32-unknown-unknown --no-default-features --features serde,runtime-wasm,watermark-time,watermark-chrono --locked
cargo bench -p sassi --bench punnu_bench --features serde,runtime-tokio --locked -- --test
cargo publish --dry-run -p sassi-codegen --locked
cargo publish --dry-run -p sassi-macros --locked
cargo publish --dry-run -p sassi --locked
cargo publish --dry-run -p sassi-cache-redis --locked
```

The `cargo lihaaf` line runs the compile-fail / compile-pass fixture suite
under `sassi-macros/tests/lihaaf/`. It replaces the earlier `trybuild`-driven
fixtures and is now the authoritative gate for proc-macro derive errors and
`MonotonicWatermark` trait-bound rejections. Install once locally with
`cargo install lihaaf --version 0.1.0-alpha.4 --locked`; CI installs it
through the same pin. To re-bless snapshots after an intentional diagnostic
change, run `cargo lihaaf --manifest-path sassi-macros/Cargo.toml --bless`
and review the resulting `.stderr` diff before committing.

For the first publish of a new version, downstream dry-runs cannot resolve until
the upstream crate exists on crates.io. Publish or dry-run in dependency order:
`sassi-codegen`, then `sassi-macros`, then `sassi`, then `sassi-cache-redis`.
After each upstream publish is visible in the registry index, rerun the next
dry-run cleanly before publishing it.

Benchmark documentation lives in
[sassi/benches/README.md](../sassi/benches/README.md). The current expectation
is that benchmarks are release baselines for comparing same-host changes, not
portable absolute performance guarantees.
