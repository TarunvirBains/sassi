# Sassi Public Docs

These docs are for Rust adopters evaluating or wiring Sassi into an application.
They sit between the crate landing page and the API reference: practical enough
to copy from, but direct about the tradeoffs that matter before a cache becomes
part of production behavior.

Start here:

- [Getting Started](getting-started.md) walks through a minimal `Cacheable`
  model, `Punnu<T>` construction, reads, scopes, `get_or_fetch`, portable JSON
  fields, and the strict wire helper gate.
- [Concepts](concepts.md) explains the core model: identity maps, predicates,
  `JSahibON`, `MemQ`, refreshers, delta sync, backends, and the `Sassi`
  orchestrator.
- [Query And Refresh Boundaries](query-refresh-boundaries.md) covers the most
  important design boundary: a `Punnu<T>` is a resident union identity map, not
  one hidden query result.
- [Backends And Runtimes](backends-and-runtimes.md) describes L1/L2 behavior,
  built-in backends, Redis, native Tokio, WASM, and framework integration
  boundaries.
- [Advanced Guide](advanced-guide.md) covers the more nuanced parts of the
  public surface: walking predicate trees (`FieldPredicate`, `LookupOp`,
  `value_as`), JSON predicate payloads, `PunnuScope` chaining, `MemQ`
  terminals, `#[trait_impl]` registry behavior, delta refresh handle
  operations, snapshot/restore modes, and custom-backend implementer notes.
- [Dependency Footprint](dependency-footprint.md) records the transitive
  dep graph by feature combination (default native, no-default, serde,
  runtime-tokio, runtime-wasm, serde-json-bridge) so adopters can audit binary
  size and supply-chain surface without running cargo.
- [Release Readiness](release-readiness.md) records the v0.1.0-beta.3 scope,
  known deferrals, issue categories, and verification commands.
- [Bardownski TUI Showcase](../examples/bardownski/README.md) is the in-repo
  native example for predicate algebra and cross-type trait queries over
  hockey shot data.
- [Benchmarks](../sassi/benches/README.md) explains the Criterion harness and
  same-host baseline workflow.

The `docs/superpowers/` tree is local planning and review material used while
building Sassi. It is useful project history, but it is not the public adopter
documentation set and may discuss speculative or superseded designs.
