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
- `proptest`, `criterion`, `tempfile`, `wasm-bindgen-test`,
  `serde_json` are dev-only. Compile-fail / compile-pass coverage is
  driven by the standalone `cargo lihaaf` cargo subcommand (installed
  in CI; not a Sassi dependency), not by an in-tree dev-dep.

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

### `--no-default-features --features serde-json-bridge`

L1 cache plus Sassi's binary value wire and explicit edge conversions between
`JSahibON` and `serde_json::Value`.

Adds vs. `--no-default-features`:

- `serde`, `postcard`, `serde_json`

Use this when the application boundary already receives or emits
`serde_json::Value`, but the cache model stores raw JSON as `JSahibON` for
portable wire behavior and local JSON predicates.

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

The sensitive-info workflow is event-driven rather than part of push CI: it
scans GitHub issue, pull request, and review text with
`cargo xtask sensitive-info --github-event "$GITHUB_EVENT_PATH"`. The release
preflight also runs `cargo xtask sensitive-info --path .` against repository
text.

## Recent Footprint Changes

- **JSON removed from the default production graph.** Sassi's core code paths used
  `serde_json` to encode backend storage keys. That dependency moved to
  postcard so the `serde` feature no longer pulls JSON into the production
  graph. `serde_json` remains opt-in through `serde-json-bridge`, remains a
  dev-dependency for the `cross_version_compat` integration tests, and remains
  in `sassi-cache-redis` for Redis id keys and pub/sub messages.
- **WASM dev-deps split out.** `proptest` and `criterion` are native-only
  dev-dependencies because their transitive graph (`rusty-fork` ->
  `wait-timeout`, `criterion`'s process model) does not build cleanly
  on `wasm32-unknown-unknown`. `wasm-bindgen-test` is a wasm-only dev
  dep. Compile-fixture coverage no longer lives in the dev-dep graph
  at all: it moved to `cargo lihaaf` (a separately installed cargo
  subcommand), configured from `sassi-macros/Cargo.toml`'s
  `[package.metadata.lihaaf]` block. The earlier `trybuild` dev-dep
  was removed in the lihaaf migration.
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
