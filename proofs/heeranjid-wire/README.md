# HeeRanjID Wire Proof

An explicit, opt-in proof harness that verifies Sassi cacheable models keyed
by HeeRanjID IDs round-trip through Sassi's existing postcard value wire and
through `Punnu` id-keyed lookup, for all four core HeeRanjID ID types:
`HeerId`, `HeerIdDesc`, `RanjId`, `RanjIdDesc`.

## Why this lives outside Sassi's default workspace

Sassi is intentionally decoupled from any specific ID library, ORM, or
storage layer (see `CLAUDE.md` — "the 'no djogi pressure' gate"). Adding
HeeRanjID as a default Sassi dependency — even a `dev-dependency` reachable
from `cargo test -p sassi` — would pull `heeranjid` and its transitive
`uuid` graph into Sassi's root `Cargo.lock`, leaking a sibling-coupling into
adopters that have never heard of HeeRanjID.

This harness sidesteps that by declaring its own top-level `[workspace]` in
`Cargo.toml`, which makes it invisible to the outer Sassi workspace's
`members` resolver and gives it its own `Cargo.lock`. The proof is run
**only** by explicit `--manifest-path` invocation; default Sassi commands
(`cargo build`, `cargo test`, `cargo test -p sassi`, `cargo clippy
--workspace`) remain completely independent and do not see `heeranjid` or
`uuid` in their dependency closure.

## Expected sibling checkout

The proof depends on a sibling HeeRanjID checkout through a relative path
(`../../../HeeRanjID/heeranjid`). To run it, both repos must be checked out
side by side:

```
<parent>/
  sassi/             ← this repo
    proofs/heeranjid-wire/
  HeeRanjID/         ← sibling, capital H/R/ID
    heeranjid/
```

The sibling directory name is `HeeRanjID` (matching the upstream repo
name), and the depended-on crate inside it is `heeranjid`.

## Running the proof

From the Sassi repo root, with both siblings checked out:

```sh
cargo test --manifest-path proofs/heeranjid-wire/Cargo.toml
```

This runs two integration tests:

- `sassi_wire_round_trips_cacheable_models_with_heeranjid_ids` — encodes
  each of the four ID-typed models through `sassi::wire::to_vec` and
  decodes back through `sassi::wire::from_slice`, asserting byte-exact
  round-trip equality.
- `punnu_looks_up_cacheable_models_by_heeranjid_ids` — inserts each model
  into a `Punnu<T>` and re-fetches by `Cacheable::id`, asserting the
  cached value equals the inserted value.

## What this proof does *not* prove

This harness exercises Sassi's existing wire and cache surface against
HeeRanjID-typed IDs as they stand today. It is intentionally narrow:

- No portable JSON encoding (that is Sassi issue #22).
- No wire marker / discriminator design (that is Sassi issue #23).
- No HeeRanjID-aware behavior in Sassi itself — the four ID types are
  treated as ordinary `serde`-derived field types.
