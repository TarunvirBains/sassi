# Sassi `JSahibON` Portable JSON Design v3

**Date:** 2026-05-14
**Status:** v3 Sassi-owned draft after v2 split review
**Owning repo:** `/home/tarunvir/projects/sassi/`
**Issue:** https://github.com/TarunvirBains/sassi/issues/22
**Companion spec:** `/home/tarunvir/projects/djogi/docs/spec/mirjzson-jsonb-integration.md`

This file is the Sassi-owned half of the JSON query design. It defines the
portable JSON value model, local predicate semantics, postcard-compatible serde
shape, and issue body for Sassi. It intentionally contains no PostgreSQL
operators, Djogi macros, SQL casts, or Djogi issue mechanics.

## Goal

Sassi adds a portable JSON value type and predicate algebra so raw JSON fields in
local Sassi/Punnu caches can be queried by values and keys without depending on
`serde_json::Value` as the in-memory representation and without requiring a
self-describing wire codec.

V1 includes:

- A postcard-compatible `JSahibON` value model.
- Optional bridge conversions to and from `serde_json::Value`.
- Root, path, key, scalar, full-value, array-containment, and array-length
  predicates.
- Typed scalar leaf predicates over raw JSON paths, e.g.
  `.jsahibon().path("age").value::<u64>().gte(30)`.
- Arbitrary UTF-8 object-key navigation, not only dotted ASCII paths.
- A single inspectable predicate payload shape under `LookupOp::Json`.

V1 does not include:

- JSONPath, recursive descent, regex, locale-aware string ordering, array index
  path syntax, or a predicate wire protocol.
- Schema-derived typed JSON path trees. V1 has typed scalar leaves over raw JSON
  paths; a future `JSahibONSchema`-style derive may add Djogi-like typed schema
  paths once the raw portable contract is stable.
- Any database-specific behavior. Downstream crates may lower the Sassi
  contract, but Sassi owns only portable local semantics.

## Cache Boundary

`JSahibON` is a Sassi wire/cache type. Foreign database wrappers are not
portable cache types merely because they serialize as JSON.

If a backend model field is `djogi::Jsonb<T>` or another database-owned JSON
wrapper, Sassi does not implicitly downcast it during cache insertion or wire
decode. The backend cache value must use an explicit Sassi-owned projection:

- Use `T` when the frontend/cache needs only the typed schema content.
- Use `JSahibON` when the frontend/cache needs the full raw JSON document,
  unknown fields, or local JSON predicates.
- Use a future Sassi typed-JSON wrapper only after Sassi owns one; v1 does not
  define schema-derived typed JSON cache fields.

This boundary keeps postcard-compatible frontend deserialization honest. A type
whose serde implementation depends on `serde_json::Value` or
`deserialize_any` is not a Sassi portable wire type unless it first projects
into `JSahibON`.

## Value Model

Sketch:

```rust
pub enum JSahibON {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(JFiniteF64),
    String(String),
    Array(Vec<JSahibON>),
    Object(JObject),
}

#[repr(transparent)]
pub struct JFiniteF64(f64); // private field

pub struct JObject {
    // implementation detail: insertion-ordered map or Vec<(String, JSahibON)>
}
```

Required traits:

- `JSahibON: Clone + Debug + PartialEq + Send + Sync + 'static`.
- `JSahibON: Serialize + Deserialize` when Sassi's `serde` feature is enabled.
- `JSahibON` must not implement `PartialOrd`.
- `JSahibON` must not implement `Eq` or `Hash` in v1. The current shape is
  reflexive after `JFiniteF64`; withholding `Eq`/`Hash` preserves room for
  future number variants and keeps hashing out of the v1 contract.

`JSahibON` is not a `serde_json::Value` newtype. It is a closed enum so postcard
can deserialize it without `deserialize_any`.

## Finite Floats

Sassi must not expose a public raw `Float(f64)` variant or any public
constructor that can store `NaN`, `+Infinity`, or `-Infinity`.

`JFiniteF64` is the only float carrier:

```rust
impl JFiniteF64 {
    pub fn try_new(value: f64) -> Result<Self, JSahibONError>;
    pub fn get(self) -> f64;
}

impl TryFrom<f64> for JFiniteF64 {
    type Error = JSahibONError;
}

impl JSahibON {
    pub fn try_f64(value: f64) -> Result<Self, JSahibONError>;
}
```

Serde requirements:

- `Serialize` emits the inner finite `f64`.
- `Deserialize` rejects any decoded non-finite `f64`, including malicious
  postcard bytes.
- Bridge conversion from `serde_json::Value` rejects non-finite numbers if a
  source ever exposes them.

Equality requirements:

- `JFiniteF64` equality uses normal finite `f64` equality, so `0.0 == -0.0`.
- There is no `NaN`, so float equality is reflexive.
- Ordering is available only through scalar JSON predicate comparisons, not
  through `PartialOrd` on `JSahibON`.

## Scalar Set And Numeric Semantics

V1's scalar set is exact:

```rust
pub trait JScalar: private::Sealed {}
pub trait JOrderedScalar: JScalar {}

// JScalar impls: i64, u64, f64, String, bool.
// JOrderedScalar impls: i64, u64, f64.
```

There are no narrow integer scalar impls in v1. Callers widen `i8`, `u16`,
`i32`, etc. at the call site. This keeps the AST and downstream lowering stable:

```rust
#[non_exhaustive]
pub enum JScalarKind { I64, U64, F64, String, Bool }

#[non_exhaustive]
pub enum JScalarValue {
    I64(i64),
    U64(u64),
    F64(JFiniteF64),
    String(String),
    Bool(bool),
}
```

JSON has one grammar-level number type; `JSahibON` stores it in three safe
carriers:

- `I64(i64)` for exact signed integers.
- `U64(u64)` for exact unsigned integers not represented as `I64`.
- `F64(JFiniteF64)` for finite binary64 numbers.

Scalar predicate matching:

- Numeric predicates match JSON numbers only. Strings, booleans, nulls, arrays,
  and objects are type mismatches and evaluate to `false`.
- `value::<i64>()`, `value::<u64>()`, and `value::<f64>()` compare in the same
  portable numeric domain. `value::<u64>()` supports the full `u64` range.
- Integer/integer comparison is exact.
- Integer/float equality succeeds only when the finite float represents the
  same mathematical integer. Ordering compares mathematical numeric values
  without panicking.
- Non-finite operands are rejected at predicate construction.
- `value::<String>()` and `value::<bool>()` support only `eq`, `neq`, `in_`, and
  `not_in`.
- No string ordering ships in v1.
- No string/number/bool coercions are implicit. `"30"` is not number `30`;
  `"true"` is not boolean `true`.

## Equality

`JSahibON`, `JObject`, and `JScalarValue` must use manual equality. Derived enum
or map equality is not sufficient.

`JSahibON` equality is structural and portable:

- Objects are order-insensitive for equality.
- Object insertion order is preserved for iteration, serde/wire roundtrips,
  debug output, and conversion back to `serde_json::Value`.
- Arrays are order-sensitive.
- Strings compare by Rust string equality, not locale collation.
- Numeric equality is softened across numeric variants: `I64(1)`, `U64(1)`, and
  `F64(JFiniteF64(1.0))` compare equal.
- `Null` equals only `Null`; booleans equal only the same boolean.

`JObject` construction enforces duplicate-key-free storage. If a duplicate key
is inserted, the first insertion position is preserved and the value is
replaced. Object equality is independent of insertion order.

Predicate methods that compare JSON values, including `eq_json`,
`array_contains`, and `JsonEq`, use this `JSahibON` equality. V1 does not expose
a strict "same enum variant" equality predicate.

## Serde JSON Bridge

Sassi's bridge to `serde_json` is opt-in:

```toml
[features]
serde-json-bridge = ["serde", "dep:serde_json"]
```

Required APIs under that feature:

```rust
impl TryFrom<serde_json::Value> for JSahibON {
    type Error = JSahibONError;
}

impl From<JSahibON> for serde_json::Value;

impl JSahibON {
    pub fn try_from_serializable<T: serde::Serialize>(
        value: &T,
    ) -> Result<Self, JSahibONError>;

    pub fn try_into_typed<T: serde::de::DeserializeOwned>(
        self,
    ) -> Result<T, JSahibONError>;
}
```

`serde_json::Value -> JSahibON` is fallible because the source can be wider than
Sassi's portable model. `JSahibON -> serde_json::Value` is total.

## Path And Key Navigation

Dotted paths are a convenience, not the full addressing model. JSON keys can
contain dots, hyphens, digits, empty strings, and non-ASCII text.

```rust
impl<T: 'static> Field<T, JSahibON> {
    pub fn jsahibon(&self) -> JSahibONFieldRef<T>;
}

impl<T: 'static> Field<T, Option<JSahibON>> {
    pub fn jsahibon(&self) -> JSahibONOptionFieldRef<T>;
}

impl<T> JSahibONFieldRef<T> {
    pub fn path(&self, dotted_plain_idents: &'static str) -> JSahibONPathRef<T>;
    pub fn key(&self, key: impl Into<String>) -> JSahibONPathRef<T>;
    pub fn path_segments<I, S>(&self, segments: I) -> JSahibONPathRef<T>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;
}

impl<T> JSahibONPathRef<T> {
    pub fn key(self, key: impl Into<String>) -> Self;
    pub fn path_segments<I, S>(self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;
}
```

Rules:

- `path("a.b")` is a convenience for plain identifier segments only:
  non-empty, starts with ASCII letter or `_`, continues with ASCII alphanumeric
  or `_`, max 63 bytes per segment.
- `key("content-type")`, `key("a.b")`, `key("0")`, `key("cafe")`, and `key("")`
  address literal object keys. The string is not parsed.
- `path_segments(["content-type", "a.b", "0"])` addresses those exact keys in
  order.
- No array indexing exists in v1. Segment `"0"` is an object key, not an array
  index.
- Local evaluation walks object keys exactly. Traversal into a non-object is a
  miss.

The internal path type stores owned UTF-8 segments:

```rust
pub struct JPath(Arc<[String]>); // empty path means root
```

## Root API

Sassi v1 exposes the same predicate families at root and at paths. The root is
represented as `JPath::root()` (zero segments), not as a separate AST variant.

Root semantics:

- `exists()` is true for `Field<T, JSahibON>`.
- `missing()` is false for `Field<T, JSahibON>`.
- For `Field<T, Option<JSahibON>>`, `exists()` is true for `Some(_)` and false
  for `None`; `missing()` is the opposite.
- `value::<V>()` at root compares the root scalar.
- `eq_json(value)` at root compares the full root JSON value.

## Predicate API

Sketch:

```rust
impl<T> JSahibONFieldRef<T> {
    pub fn exists(&self) -> BasicPredicate<T>;
    pub fn missing(&self) -> BasicPredicate<T>;
    pub fn is_json_null(&self) -> BasicPredicate<T>;
    pub fn is_not_json_null(&self) -> BasicPredicate<T>;

    pub fn has_key(&self, key: impl Into<String>) -> BasicPredicate<T>;
    pub fn has_any_key<I, S>(&self, keys: I) -> BasicPredicate<T>
    where I: IntoIterator<Item = S>, S: Into<String>;
    pub fn has_all_keys<I, S>(&self, keys: I) -> BasicPredicate<T>
    where I: IntoIterator<Item = S>, S: Into<String>;

    pub fn value<V: JScalar>(&self) -> JSahibONValueRef<T, V>;
    pub fn eq_json(&self, value: JSahibON) -> BasicPredicate<T>;
    pub fn neq_json(&self, value: JSahibON) -> BasicPredicate<T>;

    pub fn array_contains(&self, element: JSahibON) -> BasicPredicate<T>;
    pub fn array_len_eq(&self, len: usize) -> BasicPredicate<T>;
    pub fn array_len_gt(&self, len: usize) -> BasicPredicate<T>;
    pub fn array_len_gte(&self, len: usize) -> BasicPredicate<T>;
    pub fn array_len_lt(&self, len: usize) -> BasicPredicate<T>;
    pub fn array_len_lte(&self, len: usize) -> BasicPredicate<T>;
}

impl<T> JSahibONPathRef<T> {
    // Same surface as JSahibONFieldRef<T>, applied at the path.
}

impl<T, V: JScalar> JSahibONValueRef<T, V> {
    pub fn eq(&self, value: V) -> BasicPredicate<T>;
    pub fn neq(&self, value: V) -> BasicPredicate<T>;
    pub fn in_(&self, values: Vec<V>) -> BasicPredicate<T>;
    pub fn not_in(&self, values: Vec<V>) -> BasicPredicate<T>;
}

impl<T, V: JOrderedScalar> JSahibONValueRef<T, V> {
    pub fn gt(&self, value: V) -> BasicPredicate<T>;
    pub fn gte(&self, value: V) -> BasicPredicate<T>;
    pub fn lt(&self, value: V) -> BasicPredicate<T>;
    pub fn lte(&self, value: V) -> BasicPredicate<T>;
    pub fn between(&self, low: V, high: V) -> BasicPredicate<T>;
}
```

Truth rules:

- Missing path evaluates `false` for value, key, null, array, `eq_json`, and
  `neq_json` predicates. Use `missing()` explicitly when missing should match.
- Type mismatch evaluates `false`.
- `is_json_null()` is true only when the resolved value exists and is JSON null.
- `is_not_json_null()` is true only when the resolved value exists and is not
  JSON null.
- `has_key`/`has_any_key`/`has_all_keys` are true only on objects.
- `array_contains` and `array_len_*` are true only on arrays.
- `eq_json` and `array_contains` use manual `JSahibON` equality.

Membership order:

1. Resolve the path.
2. Confirm the resolved value matches the requested scalar kind.
3. If either step fails, both `in_` and `not_in` evaluate to `false`.
4. For an existing matching scalar, `in_([])` is `false` and `not_in([])` is
   `true`.

## Predicate AST

Sassi extends `LookupOp` with one variant:

```rust
#[non_exhaustive]
pub enum LookupOp {
    // existing variants...
    Json,
}
```

The erased `FieldPredicate` value stores `JSahibONPredicateBody` inside
`FieldPredicate`'s existing `Arc<dyn Any + Send + Sync>`. Downstream walkers use
`field.value_as::<JSahibONPredicateBody>()`. The payload is not an
`Arc<JSahibONPredicateBody>` unless an implementation deliberately chooses to
double-wrap and documents that choice.

Payload sketch:

```rust
#[non_exhaustive]
pub enum JSahibONPredicateBody {
    Exists { path: JPath },
    Missing { path: JPath },
    IsJsonNull { path: JPath },
    IsNotJsonNull { path: JPath },

    HasKey { path: JPath, key: Arc<String> },
    HasAnyKey { path: JPath, keys: Arc<[String]> },
    HasAllKeys { path: JPath, keys: Arc<[String]> },

    ScalarCompare {
        path: JPath,
        op: JCompareOp,
        scalar_kind: JScalarKind,
        operand: JScalarValue,
    },
    ScalarIn {
        path: JPath,
        scalar_kind: JScalarKind,
        operands: Arc<[JScalarValue]>,
        polarity: JInPolarity,
    },
    ScalarBetween {
        path: JPath,
        scalar_kind: JScalarKind,
        low: JScalarValue,
        high: JScalarValue,
    },

    JsonEq { path: JPath, value: JSahibON },
    JsonNeq { path: JPath, value: JSahibON },

    ArrayContains { path: JPath, element: JSahibON },
    ArrayLen { path: JPath, op: JCompareOp, len: u64 },
}

#[non_exhaustive]
pub enum JCompareOp { Eq, Neq, Gt, Gte, Lt, Lte }

#[non_exhaustive]
pub enum JInPolarity { In, NotIn }
```

Sassi also exposes a pure evaluator:

```rust
pub fn evaluate_jsahibon_predicate(
    root: Option<&JSahibON>,
    body: &JSahibONPredicateBody,
) -> bool;
```

Downstream crates may reuse this evaluator. They must not copy or reinterpret
Sassi truth rules.

## Sassi Tests

Required tests:

- `JSahibON` postcard roundtrip for every variant, including object order.
- `JFiniteF64` rejects `NaN`, `+Infinity`, and `-Infinity` through constructors
  and serde deserialization.
- `serde_json::Value -> JSahibON -> serde_json::Value` bridge roundtrips valid
  portable values and rejects non-portable numbers.
- Object equality is order-insensitive; iteration/wire/debug order remains
  insertion order.
- Manual numeric equality covers `I64(1) == U64(1) == F64(1.0)`, `0.0` vs
  `-0.0`, and non-finite rejection.
- `array_contains(JSahibON::I64(1))` matches an array element stored as
  `F64(1.0)` because JSON value equality uses numeric softening.
- Scalar set compile coverage proves only `i64`, `u64`, `f64`, `String`, and
  `bool` are accepted in v1.
- String ordering methods are absent or fail to compile.
- Root `exists`/`missing`/`value::<V>()` semantics for required and optional
  fields.
- Arbitrary key navigation for `content-type`, `a.b`, `0`, empty string, and
  non-ASCII keys.
- Type mismatch false semantics for key, value, array, null, membership, and
  JSON equality predicates.
- `in_([])` and `not_in([])` identities apply only after path/scalar-kind
  resolution.
- Snapshot/restore preserves `JSahibON` fields in cached values.

## Sassi Issue Body

Suggested issue title:

`Add JSahibON portable JSON value model and predicate algebra`

Issue body should summarize:

- Portable `JSahibON` value model.
- Finite `JFiniteF64` invariant.
- `serde_json::Value` bridge as `TryFrom`/`From`.
- Manual structural equality, including order-insensitive objects and numeric
  softening.
- Exact scalar set: `i64`, `u64`, `f64`, `String`, `bool`.
- Root and path APIs, including arbitrary UTF-8 key segments.
- Scalar semantics: numeric portable domain, no string ordering, no
  string/number/bool coercions.
- `LookupOp::Json` plus `JSahibONPredicateBody`.
- Sassi local tests and postcard/snapshot roundtrips.
- Explicit non-goals: schema-derived typed path trees in v1, database behavior,
  JSONPath, regex, array indexing, predicate wire protocol.
