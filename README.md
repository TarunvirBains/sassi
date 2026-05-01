# sassi

**Sassi** is a typed in-memory pool with composable predicate algebra and cross-runtime trait queries — designed for cross-runtime use (backend or Dioxus frontend) without coupling to a particular ORM, web framework, or storage layer. The `runtime-wasm` feature compiles sassi clean against `wasm32-unknown-unknown`; per-test wasm execution and full-CI matrix expansion track [issue #3](https://github.com/TarunvirBains/sassi/issues/3) (see Status below).

## What it gives you

- **`Punnu<T>`** — typed pool holding `Arc<T>` entries by `Cacheable::Id`, with bounded LRU eviction, opt-in TTL, optional pluggable L2 backend (Redis, Postgres, file, in-memory, GPU memory — backends are a trait, not a hardcoded list).
- **`BasicPredicate<T>`** — universal predicate algebra (`&`, `|`, `^`, `!`) that runs identically on backend (lowers to SQL via consumer) and frontend (evaluates against `&T` in memory).
- **`MemQ<T>`** — in-memory-only extension algebra for closures and trait-impl predicates that can't be expressed in SQL.
- **`#[sassi::trait_impl(MyTrait)]`** — register a trait impl for cross-type queries: ask "all cached entries impl-ing `MyTrait`" across every `Punnu` in a process.
- **Cross-runtime semantics** — same predicates round-trip across backend → wire → frontend via `Serialize` envelopes; visages cached on the frontend without giving the frontend any backend dependency.

## Status

**Pre-v0.1.0 alpha.** This repository is currently a skeleton; implementation lands per the design spec under `docs/superpowers/specs/` (local-only). v0.1.0 ships in lockstep with the [djogi](https://github.com/TarunvirBains/djogi) framework's v0.1.0 cut.

- **WASM target** — sassi compiles clean for `wasm32-unknown-unknown` under the `runtime-wasm` feature; the `wasm-target` CI job pins the contract. Per-test wasm execution (`wasm-bindgen-test` runner) and the full multi-target CI matrix track [issue #3](https://github.com/TarunvirBains/sassi/issues/3).

## Workspace layout

```
sassi/                ← main library crate (Punnu, BasicPredicate, MemQ, Sassi orchestrator, CacheBackend trait)
sassi-codegen/        ← shared codegen library (TokenStream emitters used by sassi-macros AND djogi-macros)
sassi-macros/         ← proc-macros (#[derive(Cacheable)], #[sassi::trait_impl])
```

`sassi-codegen` exists because proc-macro crates can't depend on each other directly — both `sassi-macros` and djogi's `djogi-macros` need a shared place to emit `Cacheable` field-struct codegen without drifting. It's a regular library crate, not a proc-macro crate.

## Naming

Sassi-Punnu is the legendary Punjabi tragic-romance pair, alongside Heer-Ranjha (which the sibling [HeeRanjID](https://github.com/TarunvirBains/HeeRanjID) crate carries) and djogi-maahi (in the [djogi](https://github.com/TarunvirBains/djogi) repo as `djogi` + the `djogi-maahi` admin crate). The tradition: each crate's name is something a seeker calls a beloved.

If you're looking for backronym credibility: **Sassi** = Smart Asynchronous Storage Interface; **Punnu** = Predictive Universal Node Network Utility. Both work; the names came first.

## License

Dual-licensed under [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE) — Rust ecosystem standard. Pick whichever fits your project.

## Contributing

Pre-v0.1.0; design churn is expected. The implementation plan (once it lands in `docs/spec/`) will list contributor on-ramps. The internal `PunnuExecutor` abstraction is the obvious first one — currently `pub(crate)` with `cfg`-gated tokio / wasm-bindgen-futures impls; v0.2 promotes to `pub` so consumers can plug in custom executors.
