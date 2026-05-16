# Cacheable Postcard Wire Guard Design v1

## Decision

Sassi issue #23 adds an additive, opt-in compile-time guard for entry types that
claim to be portable over Sassi's existing postcard-backed binary wire. The
guard must not change the wire format, the wire major, header validation, or the
existing loose wire APIs.

The v1 design keeps `sassi::wire::to_vec` and `sassi::wire::from_slice` exactly
as the compatibility surface for existing callers:

- `to_vec<T>` remains available for `T: Cacheable + Serialize`.
- `from_slice<T>` remains available for `T: Cacheable + DeserializeOwned`.
- Existing L2 and snapshot paths keep their current bounds in v1.

The new surface is a strict opt-in layer:

- `SassiWire`: a conservative marker for field, id, and value component types
  accepted for Sassi postcard wire portability.
- `WirePortable`: a marker for complete `Cacheable` entry types that satisfy the
  entry-level contract.
- `#[cacheable(wire_portable)]`: a derive opt-in that emits compile-time checks
  for `Self::Id` and every named field type, then emits the `WirePortable` entry
  marker.
- `to_vec_portable` and `from_slice_portable`: strict helper functions that
  require `WirePortable` and delegate byte-identically to the existing helpers.

This is intentionally not a proof that serde implementations are safe or fully
portable. It rejects known bad shapes by trait absence, but manual marker impls
can lie, and a derive macro cannot inspect the body of a foreign
`Deserialize` impl to prove absence of `deserialize_any`.

This design is derived from the cache-boundary rule in
`docs/specs/2026-05-14-jsahibon-portable-json-design-v3.md`: `JSahibON` is the
Sassi-owned portable wire/cache JSON type; `serde_json::Value`,
`deserialize_any`-dependent wrappers, and database-owned JSON wrappers are not
portable cache-wire types merely because they serialize as JSON. Issue #23
lifts that rule into a general opt-in wire guard before the larger JSahibON work
continues.

Sassi #22 (JSahibON portable JSON) is active and lands with or immediately
adjacent to issue #23. Issue #23 itself does not add JSahibON, JSON predicates,
or the serde-json bridge — those ship under #22. The two issues coordinate so
`JSahibON`, `JFiniteF64`, and `JObject` implement `SassiWire`, and
`#[cacheable(wire_portable)]` entries with `JSahibON` and `Option<JSahibON>`
fields pass the guard without application-owned manual markers.

## Public API

The canonical public API lives under the existing serde-gated `sassi::wire`
module:

```rust
pub trait SassiWire: Serialize + DeserializeOwned + Send + Sync + 'static {}

pub trait WirePortable:
    Cacheable + Serialize + DeserializeOwned + Send + Sync + 'static
where
    <Self as Cacheable>::Id: SassiWire,
{
}

pub fn to_vec_portable<T>(payload: &T) -> Result<Vec<u8>, WireFormatError>
where
    T: WirePortable,
    <T as Cacheable>::Id: SassiWire;

pub fn from_slice_portable<T>(bytes: &[u8]) -> Result<T, WireFormatError>
where
    T: WirePortable,
    <T as Cacheable>::Id: SassiWire;
```

`WirePortable` is the chosen entry marker name for v1. Do not add both
`WirePortable` and `CacheWirePortable`; one public name keeps diagnostics and
documentation simpler.

`SassiWire` implementations are explicit and conservative. v1 should include
known postcard-safe primitives and standard containers whose elements also
implement `SassiWire`, for example:

- `()`, `bool`, `char`, fixed-width integers, `f32`, `f64`, `String`.
- `Option<T>`, `Result<T, E>`, `Vec<T>`, `Box<T>`, arrays, and tuples up to the
  arity already supported by local macro style, all bounded by `SassiWire`.
- Ordered containers such as `BTreeMap<K, V>` and `BTreeSet<T>`, bounded by
  `SassiWire`.
- Explicit user-owned newtypes with manual `impl SassiWire`.

There must be no blanket implementation for `T: Serialize + DeserializeOwned`.
That blanket would make the marker cosmetic and would fail to reject
`serde_json::Value`, JSON-backed wrappers, and other shapes that postcard may
not support portably.

The helpers are additive convenience and policy APIs. They do not replace the
existing helpers and do not become the backend contract in v1.

## Trait And Bounds Model

The model has two contracts.

`SassiWire` is the component contract. It says a type is accepted as part of a
Sassi postcard wire entry when it appears as an id type, field type, or nested
container member. Sassi owns the standard library allowlist. Application crates
own any manual newtype implementations they add.

`WirePortable` is the complete-entry contract. It says a `Cacheable` entry type
has passed either the derive contract or an equivalent manual review. It is the
bound used by strict helper functions.

The split matters because a complete entry type is not just one serde type. A
derived `Cacheable` entry has:

- `Self`, which must serialize and deserialize as the payload.
- `Self::Id`, which participates in cache identity and L2 key material.
- Named fields, which are the shape the derive can inspect and where JSON-like
  wrappers should fail early.

The derive contract must check `Self::Id: SassiWire` and every named field
type: `SassiWire`. Checking fields gives useful failures for
`serde_json::Value` and djogi-like `Jsonb<T>` wrappers unless the user projects
the entry to portable fields, wraps the data in an audited newtype, or manually
marks the wrapper.

For JSON specifically, this mirrors the JSahibON v3 cache-boundary guidance:
frontends and local Punnu caches should receive `JSahibON` when they need raw
JSON, `T` when they need only typed schema content, or a future Sassi-owned
typed JSON wrapper after Sassi defines one. Sassi should not implicitly
downcast `djogi::Jsonb<T>` or another database wrapper during cache insertion or
wire decode.

Manual `impl SassiWire` and manual `impl WirePortable` are escape hatches, not
proofs. Documentation must say plainly that these impls are assertions by the
implementor. They can be wrong, and Sassi cannot verify the behavior of custom
or foreign serde code.

## Derive And Codegen

Ownership boundaries:

- `sassi-codegen` owns parsing and code generation for the
  `#[cacheable(wire_portable)]` option.
- `sassi-macros` owns the proc-macro entry point that calls `sassi-codegen` and
  combines generated tokens.
- `sassi` owns the runtime traits, standard `SassiWire` implementations, strict
  helper functions, rustdoc, and public module paths.
- `docs` owns user-facing concept, backend, release-readiness, and changelog
  updates.

`sassi-codegen/src/derive_options.rs` should add a `wire_portable` boolean or
span-carrying option to `CacheableDeriveOptions`. It should parse only the bare
struct-level form:

```rust
#[cacheable(wire_portable)]
```

It must reject:

- Duplicate `wire_portable` options.
- Value form, such as `#[cacheable(wire_portable = true)]`.
- List form, such as `#[cacheable(wire_portable(...))]`.
- Field-level `#[cacheable(...)]` attributes, preserving the existing rule.
- Unknown options, preserving the existing rejection behavior.

`sassi-codegen` should add a generation path such as
`generate_wire_portable_impl`. When `wire_portable` is absent, it emits no tokens.
When present, it emits:

- A compile-time assertion for `<Self as Cacheable>::Id: SassiWire`.
- A compile-time assertion for every named field type: `SassiWire`.
- An entry marker impl for `WirePortable`.

Field assertions should be emitted with spans tied to the field type where
practical, so diagnostics point at the bad field instead of only the struct
attribute. The id assertion should reference `Self::Id` even though the current
derive also checks the `id` field as one of the named fields; that makes the
entry contract explicit and survives future id-field customization.

The derive may also assert `Self: Serialize + DeserializeOwned` using a
`sassi::__private` serde re-export or the `WirePortable` supertrait path, so a
`wire_portable` opt-in without serde derives fails near the opt-in site instead
of much later. This is a diagnostic-quality requirement, not a wire-format
change.

`sassi-macros/src/cacheable.rs` should keep the current orchestration pattern:
parse options once, generate fields, generate `Cacheable`, generate optional
delta-sync, generate optional wire-portable, and quote the combined output. It
should not duplicate the `wire_portable` parsing rules.

## Existing Wire Integration

`sassi/src/wire.rs` already owns the postcard-backed binary value container and
header validation. The new helpers must live there, behind the existing
`feature = "serde"` module gate:

```rust
pub fn to_vec_portable<T>(payload: &T) -> Result<Vec<u8>, WireFormatError>
where
    T: WirePortable,
    <T as Cacheable>::Id: SassiWire,
{
    to_vec(payload)
}

pub fn from_slice_portable<T>(bytes: &[u8]) -> Result<T, WireFormatError>
where
    T: WirePortable,
    <T as Cacheable>::Id: SassiWire,
{
    from_slice(bytes)
}
```

The output must be byte-identical to `to_vec` for the same payload. The input
validation must be byte-identical to `from_slice`: magic, wire major, kind,
flags, cache type name, postcard body decode, and trailing-byte rejection stay
unchanged.

No new header kind, flag, body envelope, or wire major is added in v1. The
strict helpers only change compile-time admissibility.

Existing L2 and snapshot paths remain loose in v1:

- `MemoryBackend`.
- `FileBackend`.
- `Punnu::insert_serialized`.
- `Punnu::export_entries_postcard`.
- `Punnu::restore_entries_postcard`.
- `Punnu::snapshot_postcard`.
- `Punnu::restore_postcard`.
- Backend trait and builder bounds.

Their current loose serde bounds remain in place: `T: Cacheable + Serialize` or
`T: Cacheable + DeserializeOwned` as each API requires, and `T::Id:
Serialize + DeserializeOwned` wherever the backend or snapshot contract already
requires it.

A future ratchet may add `WirePortable` bounds to backend and snapshot APIs, but
v1 must not break existing users who depend on the current serde bounds.

## Lihaaf And Test Coverage

Compile fixtures belong under `sassi-macros/tests/lihaaf`, using the existing
Lihaaf configuration in `sassi-macros/Cargo.toml`.

Required compile-pass coverage:

- A good portable entry with `#[derive(Cacheable, Serialize, Deserialize)]` and
  `#[cacheable(type_name = "...", wire_portable)]`.
- Fields covering representative allowed shapes: primitive id, `String`,
  `Option<T>`, `Vec<T>`, an ordered map or set, and a user newtype that manually
  implements `SassiWire`.
- A strict helper call using `sassi::wire::to_vec_portable` and
  `sassi::wire::from_slice_portable`.

Required compile-fail coverage:

- `serde_json::Value` field rejection because `serde_json::Value: SassiWire` is
  not implemented.
- Djogi-like `Jsonb<T>` wrapper rejection because the wrapper is not
  automatically `SassiWire`.
- A `#[cacheable(wire_portable)]` entry with `JSahibON` and `Option<JSahibON>`
  fields that passes without manual application markers (lands as
  `wire_portable_jsahibon_pass.rs` once #22 ships the types).
- Strict helper call on a `Cacheable + Serialize + DeserializeOwned` entry that
  did not opt in to `wire_portable`, proving the entry marker is meaningful.
- Duplicate `wire_portable` option rejection.
- Value-form `wire_portable = true` rejection.
- Field-level `#[cacheable(wire_portable)]` or related field-level cacheable
  option rejection, preserving the existing no-field-level-attrs rule.
- Field-level diagnostics where the emitted error points at the offending field
  type for unsupported field shapes.

Snapshot `.stderr` files should assert stable, user-facing diagnostic text. They
do not need to assert every rustc note, but they should lock the primary message
and enough span context to prevent regressions to struct-only diagnostics.

Runtime tests for byte identity are useful in `sassi` when implementation starts,
but they are not part of this current spec edit. This document intentionally does
not require running tests as part of the spec-writing task.

## Documentation Updates

Docs ownership is separate from code ownership. The implementation should update:

- Concepts documentation: define Sassi postcard wire portability, `SassiWire`,
  `WirePortable`, and the limits of compile-time checking.
- Backend documentation: state that existing backend and snapshot APIs still use
  loose serde bounds in v1, while strict helpers are available for adopters that
  want an opt-in guard.
- Release-readiness documentation: record this as an additive guard and list the
  remaining future ratchet decision for backend and snapshot bounds.
- Changelog: note the new derive option, marker traits, strict helpers, and the
  fact that no wire bytes or existing bounds changed.

Docs must include false-confidence warnings:

- Trait absence rejects known bad shapes.
- Manual marker impls can lie.
- Derive cannot inspect foreign serde bodies or prove absence of
  `deserialize_any`.

Docs should also show the intended remediation path for rejected JSON-like
fields: project the cache entry to portable fields, use an audited newtype with a
manual marker impl, or keep using the loose APIs when portability is not claimed.

## Non-goals

- Do not break existing loose `to_vec` or `from_slice` callers.
- Do not change the postcard-backed binary wire format.
- Do not add a JSON or hybrid wire format in issue #23; JSahibON types added
  under issue #22 are only expected to satisfy this contract through their
  `SassiWire` impls.
- Do not make `SassiWire` a blanket alias for serde traits.
- Do not claim the marker proves serde implementation behavior.
- Do not add `WirePortable` bounds to `MemoryBackend`, `FileBackend`, Punnu
  snapshot/restore APIs, backend traits, or builder APIs in v1.
- Do not expand derive support to generic structs as part of this issue; the
  current derive rejection remains in force.

## Open Questions

- The initial `SassiWire` allowlist should be finalized before implementation.
  The recommended conservative default is fixed-width scalar types, strings,
  option/result, vec, arrays, tuples, boxes, and ordered maps/sets. `usize`,
  `isize`, hash maps, hash sets, and time/domain types should require an
  explicit decision rather than slipping in through a blanket impl.
- Should the serde feature-disabled diagnostic be improved beyond rustc's
  "could not find `wire` in `sassi`" message when a downstream crate uses
  `#[cacheable(wire_portable)]` without enabling Sassi's `serde` feature?
- When should the future backend/snapshot ratchet happen, and should it be a
  major wire-policy change or a staged deprecation?
- The core `JSahibON` value and Sassi-owned supporting container/value types
  satisfy `SassiWire` directly under #22. Application/database wrappers
  around JSON still require projection, audited newtypes, or manual markers.

## Implementation Notes

- Add `SassiWire` and `WirePortable` to `sassi/src/wire.rs` so they are gated
  with the existing wire module and can use serde/postcard-related bounds.
- Consider re-exporting serde through `sassi::__private` behind
  `feature = "serde"` if macro-generated assertions need stable paths to
  `Serialize` and `DeserializeOwned`.
- Implement standard `SassiWire` impls with small local macros to keep the
  allowlist readable. Container impls must be recursively bounded by
  `SassiWire`.
- Keep `WirePortable` unsealed so manually implemented `Cacheable` entry types
  can opt in after an application-level audit.
- Add rustdoc to both marker traits that explains the false-confidence limits
  and the absence of a blanket serde impl.
- Extend `CacheableDeriveOptions` with a span-carrying `wire_portable` option so
  duplicate and malformed attribute errors can point at the option.
- Emit field-type assertions with `quote_spanned!` or equivalent span handling.
- Add `generate_wire_portable_impl` to `sassi-codegen` and export it from
  `sassi-codegen/src/lib.rs`.
- Update `sassi-macros/src/cacheable.rs` to include the new generated token
  stream in the existing derive output.
- Add Lihaaf fixtures and snapshots under the existing compile-pass and
  compile-fail directories.
- Do not touch backend bounds in the v1 implementation except for docs that
  explicitly say the future ratchet is out of scope.

## Acceptance Criteria

- `sassi::wire::SassiWire` exists with a conservative explicit allowlist and no
  blanket serde implementation.
- `sassi::wire::WirePortable` exists as the complete-entry marker.
- `sassi::wire::to_vec_portable` and `sassi::wire::from_slice_portable` exist,
  require `WirePortable`, and delegate to the existing helpers without changing
  bytes or validation behavior.
- Existing `to_vec`, `from_slice`, backend, builder, and Punnu snapshot/restore
  bounds remain unchanged in v1.
- `#[cacheable(wire_portable)]` parses only as a bare struct-level option.
- The derive emits compile-time assertions for `Self::Id: SassiWire` and every
  named field type: `SassiWire`.
- The derive emits `impl WirePortable for Self` only for opt-in entry types.
- Lihaaf covers good portable entries, `serde_json::Value` rejection,
  djogi-like `Jsonb<T>` rejection, missing marker at strict helper call,
  duplicate/value-form attribute errors, field-level attribute rejection, and
  field-level diagnostics.
- The spec explicitly preserves the JSahibON v3 cache-boundary rule: raw JSON
  cache fields should project to future `JSahibON`; database JSON wrappers and
  `serde_json::Value` remain rejected unless explicitly marked or projected.
- Concepts, backend, release-readiness, and changelog docs are updated.
- Documentation warns that this is a guardrail, not a proof: manual impls can
  lie and derive cannot inspect foreign serde bodies or prove absence of
  `deserialize_any`.
