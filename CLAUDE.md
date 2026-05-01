# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working in the sassi repo.

## What this project is

**Sassi** is a typed in-memory pool with composable predicate algebra and cross-runtime trait queries. Standalone Rust crate; intentionally **decoupled** from any specific web framework, ORM, or storage layer. The sibling [djogi](https://github.com/TarunvirBains/djogi) framework consumes sassi as one of several integrated siblings (alongside HeeRanjID), but sassi must remain useful to Rust consumers who have never heard of djogi.

The design spec lives at `docs/superpowers/specs/2026-04-30-sassi-design.md` (gitignored; brainstorming-stage). Once implementation begins, the spec promotes to `docs/spec/design.md` (tracked).

## Workspace layout

```
sassi/                ← main library crate
sassi-codegen/        ← shared codegen library (used by both sassi-macros AND djogi-macros)
sassi-macros/         ← proc-macros — #[derive(Cacheable)], #[sassi::trait_impl]
```

## The "no djogi pressure" gate

Sassi must justify every PR against vanilla Rust consumers — never "djogi needs this." If a PR description reads "djogi wants X," reword it for a hypothetical adopter who has never heard of djogi. The gate keeps sassi from quietly becoming a djogi sub-crate.

## Workspace conventions

- **Edition:** 2024.
- **MSRV:** Rust 1.95 (matches djogi sibling — eases shared-toolchain CI for adopters using both).
- **License:** dual MIT OR Apache-2.0 (Rust ecosystem standard). Djogi is Apache-2.0-only; sassi is broader.
- **Branch:** `main` (never `master`).
- **Pre-commit hook checks:** `cargo fmt`, `cargo clippy --all-targets --all-features -D warnings`, `cargo test`. Atomic commits — each commit is one logical unit, passes tests in isolation.

## Cross-runtime requirements

Sassi must compile and run on:
- **Native** (tokio runtime via `runtime-tokio` feature)
- **WASM** (wasm-bindgen-futures + gloo-timers via `runtime-wasm` feature)

Internal code routes spawn / sleep through `pub(crate) trait PunnuExecutor` so the runtime choice is one place. v0.2 promotes the trait to `pub` for custom executors.

## Drift prevention with djogi

`sassi-codegen` is the canonical home for `Cacheable` field-struct codegen. Both `sassi-macros::Cacheable` and `djogi-macros::Model` (which auto-derives `Cacheable` for djogi models) call into the same `sassi-codegen::generate_fields_struct(...)` so the two macro emitters cannot drift. Never duplicate codegen logic between the two repos — push it into `sassi-codegen`.

## Reading order for new sessions

1. This file (you are here).
2. `README.md` — public-facing overview + workspace layout + license.
3. `docs/superpowers/specs/2026-04-30-sassi-design.md` — full design (long; ~1360 lines). Sections of interest:
   - §1 Goals / non-goals
   - §3 Sassi public API surface (the meat)
   - §4 Djogi additions (the consumer surface; useful to know what we're being called from but **not part of sassi proper**)
   - §6.4 Wire-format envelope (cross-runtime compat)
4. `docs/spec/` — once implementation begins, the finalized spec lives here. Currently empty (skeleton phase).

## What this project is NOT

- Not a Postgres-specific cache layer (Postgres is one of several pluggable backends, not the substrate).
- Not a wrapper around djogi (sassi has zero djogi dependency; the relationship is one-way — djogi calls sassi).
- Not an "if you have an ORM, use this" convention — sassi is useful for any Rust app that wants typed in-memory caches with composable predicates, ORM or no.

## Status

**Pre-v0.1.0 alpha skeleton.** Workspace compiles to a no-op `pub fn version()` placeholder. Implementation lands per the design spec; v0.1.0 ships in lockstep with djogi v0.1.0 (sassi publishes first; djogi flips its `path = "../sassi"` dep to `sassi = "0.1"` and re-tests; both then go live).
