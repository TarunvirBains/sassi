# Dependency Footprint By Feature

This page captures Sassi's transitive dependency graph for each supported
feature combination. Use it as a sanity-check when auditing the binary size,
build-time, or supply-chain surface of a downstream application that pins
Sassi.

The graphs below are derived from `cargo tree -p sassi --no-default-features
--features <…>` against the workspace's locked `Cargo.lock`. They are
recorded here so adopters can review without running cargo, but the
canonical answer is always `cargo tree` against the version they use.

## Production Feature Combinations

### Default native (`default-features = true`)

Resolves to `serde + runtime-tokio`. This is the typical native consumer
shape: typed identity caches with the binary value wire and the optional
background TTL sweep / refresh / delta-refresh tasks.

Direct production deps:

- `tokio` (default features off; `sync,rt,time,macros` only)
- `futures`
- `serde` (via `serde` feature)
- `postcard` (via `serde` feature)
- `dashmap`, `arc-swap`, `im`, `fastrand`, `tracing`, `async-trait`,
  `thiserror`, `inventory`, `web-time`
- `sassi-macros` (compile-time only; macro expansion at build time)

Notable absent deps:

- `serde_json` is not pulled in by Sassi proper. Sassi's generic backend
  storage-key helper uses postcard. Adapters outside Sassi proper can choose
  their own external wire; `sassi-cache-redis` keeps JSON for Redis id keys
  and pub/sub invalidation messages.
- `proptest`, `criterion`, `trybuild`, `tempfile`, `wasm-bindgen-test`,
  `serde_json` are dev-only.

### `--no-default-features` (L1-only, no serde, no runtime)

The smallest production shape: in-process identity map with predicate algebra
and no background work.

Direct production deps:

- `tokio` (still — for the broadcast channel that backs the event stream;
  works without an executor)
- `futures`
- `dashmap`, `arc-swap`, `im`, `fastrand`, `tracing`, `async-trait`,
  `thiserror`, `inventory`, `web-time`
- `sassi-macros`

Notable absent deps:

- No `serde`, `postcard`, or wire support.
- No `wasm-bindgen-futures` / `gloo-timers`.
- No `tokio` runtime — `tokio::sync::broadcast` works without an active
  runtime; the executor's `spawn` / `sleep` paths panic at construction time
  if a sweep / refresh is configured without a `runtime-*` feature, so the
  failure mode is loud rather than silent.

### `--no-default-features --features serde`

L1 cache plus the binary value wire, but no background work.

Adds vs. `--no-default-features`:

- `serde`, `postcard`

Useful for libraries that re-emit Sassi payloads onto their own runtime or
that integrate with a consumer-supplied executor outside Sassi's contract.

### `--no-default-features --features serde,runtime-tokio`

L1 + wire + tokio background work. Equivalent to the default but with
explicit feature flags for adopters that pin features by name.

Same shape as the default native combo described above.

### `--no-default-features --features serde,runtime-wasm`

The `wasm32-unknown-unknown` consumer shape. Adds:

- `wasm-bindgen-futures`
- `gloo-timers` (with the `futures` feature)

`web-time` is already in the always-on dependency set; on wasm it wraps the
browser's `Performance.now()` API.

### `--no-default-features --features watermark-time,watermark-chrono`

Adds `time` and `chrono` (default features off) for the `MonotonicWatermark`
marker impls that pair with their respective timestamp types. These features
are additive; they do not enable any other behavior. Consumers that already
have `time` or `chrono` in their dep graph will deduplicate.

## Compile-Surface Verification

The CI matrix exercises the production combinations explicitly:

| Job        | Command                                                                                         |
|------------|-------------------------------------------------------------------------------------------------|
| `check`    | `cargo test --workspace`                                                                        |
|            | `cargo test -p sassi --no-default-features`                                                     |
|            | `cargo test -p sassi --no-default-features --features serde`                                    |
|            | `cargo test -p sassi --no-default-features --features serde,runtime-tokio`                      |
| `wasm-target` | `cargo build -p sassi --target wasm32-unknown-unknown` (default features)                    |
|            | `cargo build -p sassi --target wasm32-unknown-unknown --no-default-features --features runtime-tokio,runtime-wasm` |
|            | `cargo build -p sassi --target wasm32-unknown-unknown --no-default-features --features serde,runtime-wasm`         |
|            | `cargo test -p sassi --target wasm32-unknown-unknown --test punnu_executor_wasm --no-default-features --features serde,runtime-wasm` |
| `msrv`     | `cargo check --workspace` against Rust 1.95                                                     |
| `docs`     | `cargo doc --workspace --no-deps --all-features` with `RUSTDOCFLAGS=-D warnings`                |

`runtime-tokio` and `runtime-wasm` together is a meaningful build to keep
green: the features themselves do not conflict (selecting both is the right
shape for a workspace that compiles for both targets), only the wasm
executor body is gated `cfg(target_arch = "wasm32")`.

## Recent Footprint Changes

- **JSON removed from the production graph.** Sassi's core code paths used
  `serde_json` to encode backend storage keys. That dependency moved to
  postcard so the `serde` feature no longer pulls JSON into the production
  graph. `serde_json` remains a dev-dependency for the
  `cross_version_compat` integration tests, which need to fabricate beta.1
  JSON envelopes for rejection tests.
- **WASM dev-deps split out.** `proptest`, `criterion`, and `trybuild` are
  native-only dev-dependencies because their transitive graph
  (`rusty-fork` -> `wait-timeout`, `criterion`'s process model, `trybuild`'s
  cargo invocation) does not build cleanly on `wasm32-unknown-unknown`.
  `wasm-bindgen-test` is a wasm-only dev dep.
- **`serde_json` reasoning preserved for sassi-cache-redis.** The Redis
  companion crate retains `serde_json` for Redis id-to-key encoding and
  pub/sub invalidation messages. Those bytes are part of the Redis adapter's
  external storage/notification surface and benefit from a stable
  cross-language wire format. That dep stays out of Sassi proper.

## Reading Your Own Build

To audit the dep graph for a specific feature combination locally:

```bash
cargo tree -p sassi --no-default-features --features serde,runtime-tokio
cargo tree -p sassi --no-default-features --features serde,runtime-wasm \
  --target wasm32-unknown-unknown
```

`cargo tree --duplicates` is useful for spotting transitive crates that
appear at multiple major versions — a common cause of binary bloat.

```bash
cargo tree --duplicates -p sassi
```

Sassi tries to keep the duplicate set short. If a duplicate appears, please
open an issue with the output and the exact feature combination.
