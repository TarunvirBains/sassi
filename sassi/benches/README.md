# Sassi Benchmarks

`punnu_bench.rs` contains Criterion baselines for public Sassi APIs. These are
release baselines and regression signals, not hard performance guarantees.

## What is measured

- `insert`, hot `get`, and scope-style lookups (`BasicPredicate` + mixed `MemQ`).
- direct `apply_delta`.
- `get_or_fetch` hit and coalesced miss paths.
- `get_or_fetch_many`.
- sampled-LRU pressure and TTL/sweep behavior.
- wire round-trips through `sassi::wire::{to_vec, from_slice}` (when `serde` is enabled).
- file-backed backend roundtrips (when `serde` is enabled).
- `Sassi::all_impl` via a small trait-registration example.
- read-under-write stress patterns.

## Baseline intent

This harness is meant for repeatable, same-host software-change tracking:

- compare commit-to-commit deltas on the same machine and runtime,
- compare feature-flag combinations (`serde`, `runtime-tokio`) in the same environment,
- compare scheduling/machine load changes you control.

The intent is to detect regressions and major direction changes in public
surfaces, not to publish hardware-independent absolute throughput.

## Record a baseline

From the workspace root:

```bash
BASELINE="$(git rev-parse --short HEAD)-$(rustc -V | tr ' ' '_')"
cargo bench -p sassi --bench punnu_bench --features serde,runtime-tokio -- --save-baseline "$BASELINE"
rustc -Vv > target/criterion/sassi-benchmark-env.txt
uname -a >> target/criterion/sassi-benchmark-env.txt
```

For a quick compile-and-execute smoke check without collecting measurements:

```bash
cargo bench -p sassi --bench punnu_bench --features serde,runtime-tokio -- --test
```

Compare a later change on the same machine/config:

```bash
cargo bench -p sassi --bench punnu_bench --features serde,runtime-tokio -- --baseline "$BASELINE"
```

Use percentage deltas from the same host, Rust toolchain, feature set, target
profile, CPU governor, and load profile before changing write coordination,
predicate evaluation, delta application, single-flight, or wire serialization.

## Throughput claims

Do not treat these numbers as absolute performance guarantees for any external
deployment.
Results are sensitive to:

- host CPU/memory profile,
- filesystem and NUMA characteristics,
- Tokio runtime feature and scheduling pressure,
- compiler/runtime options,
- data shape and warm-up strategy,
- concurrent workload mix and process load.

Use these numbers as a consistent local baseline only.
