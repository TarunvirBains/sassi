//! JSON predicate builders and evaluator for [`JSahibON`].
//!
//! Predicates over `JSahibON` cache fields are constructed through the
//! [`Field<T, JSahibON>::jsahibon`] / [`Field<T, Option<JSahibON>>::jsahibon`]
//! extension methods and live under [`LookupOp::Json`]. The body is captured as
//! [`JSahibONPredicateBody`] so downstream walkers (debug formatters, future
//! lowering) can inspect the AST through
//! [`FieldPredicate::value_as`](crate::predicate::FieldPredicate::value_as).

use super::basic::BasicPredicate;
use super::field_predicate::{FieldPredicate, LookupOp};
use crate::cacheable::Field;
use crate::jsahibon::{JObject, JSahibON, compare_jsahibon_numbers};
use std::any::Any;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::Arc;

/// JSON path expressed as an ordered sequence of UTF-8 object key segments.
///
/// The empty sequence is the root. Segments address literal object keys —
/// dotted paths are a convenience over plain ASCII identifiers; arbitrary
/// keys that are not plain identifiers must be added with
/// [`JPath::from_segments`] or via [`JSahibONPathRef::key`] /
/// [`JSahibONPathRef::path_segments`]. There is no array indexing in v1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JPath(Arc<[String]>);

impl JPath {
    /// Construct the root path (zero segments).
    pub fn root() -> Self {
        Self(Arc::from([]))
    }

    /// Construct a path from any iterable of segment strings.
    ///
    /// Each segment is taken as a literal object key without parsing.
    pub fn from_segments<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self(
            segments
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>()
                .into(),
        )
    }

    /// Parse a dotted plain-identifier path into segments.
    ///
    /// Each segment must be a non-empty ASCII identifier (starting with an
    /// ASCII letter or `_`, continuing with ASCII alphanumerics or `_`) of at
    /// most 63 bytes. Keys containing dots, hyphens, empty strings,
    /// non-ASCII text, or an initial digit must be addressed through
    /// [`JPath::from_segments`] or [`JSahibONPathRef::key`].
    ///
    /// # Panics
    ///
    /// Panics when any segment fails the plain-identifier check above. The
    /// function is intended for `'static` literals authored at compile time;
    /// invalid input is a programmer error rather than a runtime concern.
    pub fn parse_dotted(path: &'static str) -> Self {
        let segments = path.split('.').collect::<Vec<_>>();
        assert!(
            segments.iter().all(|segment| valid_plain_segment(segment)),
            "JSahibON dotted paths require non-empty ASCII identifier segments of at most 63 bytes"
        );
        Self::from_segments(segments)
    }

    /// Return the path's segments in declaration order.
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    fn push(&self, key: String) -> Self {
        let mut segments = Vec::with_capacity(self.0.len() + 1);
        segments.extend(self.0.iter().cloned());
        segments.push(key);
        Self(segments.into())
    }

    fn extend<I, S>(&self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut next = Vec::from(self.0.as_ref());
        next.extend(segments.into_iter().map(Into::into));
        Self(next.into())
    }
}

impl Default for JPath {
    fn default() -> Self {
        Self::root()
    }
}

impl FromIterator<String> for JPath {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self::from_segments(iter)
    }
}

impl<'a> FromIterator<&'a str> for JPath {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        Self::from_segments(iter)
    }
}

fn valid_plain_segment(segment: &str) -> bool {
    let mut bytes = segment.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    if segment.len() > 63 || !(first.is_ascii_alphabetic() || first == b'_') {
        return false;
    }
    bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

/// Discriminant for the scalar variant carried in a JSON predicate operand.
///
/// V1 has exactly five accepted scalar kinds; new kinds may be added in
/// future versions, hence `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum JScalarKind {
    /// Exact signed-integer scalar.
    I64,
    /// Exact unsigned-integer scalar.
    U64,
    /// Finite binary64 scalar.
    F64,
    /// UTF-8 string scalar (`eq`/`neq`/`in_`/`not_in` only — no ordering).
    String,
    /// Boolean scalar (`eq`/`neq`/`in_`/`not_in` only).
    Bool,
}

/// Discriminant for JSON value-type assertions used by `is_type` predicates.
///
/// Each kind matches one shape of [`JSahibON`]; numeric kinds collapse the
/// three numeric carriers (`I64`, `U64`, `F64`) into a single `Number` kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum JTypeKind {
    /// Matches [`JSahibON::Null`].
    Null,
    /// Matches [`JSahibON::Bool`].
    Bool,
    /// Matches any of [`JSahibON::I64`], [`JSahibON::U64`], or [`JSahibON::F64`].
    Number,
    /// Matches [`JSahibON::String`].
    String,
    /// Matches [`JSahibON::Array`].
    Array,
    /// Matches [`JSahibON::Object`].
    Object,
}

/// Type-erased scalar operand carried inside a JSON predicate body.
///
/// Equality across numeric variants follows [`JSahibON`]'s cross-numeric
/// softening (e.g. `I64(1)` equals `U64(1)` equals `F64(1.0)`).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum JScalarValue {
    /// Exact signed integer.
    I64(i64),
    /// Exact unsigned integer.
    U64(u64),
    /// Finite binary64 value.
    F64(crate::JFiniteF64),
    /// UTF-8 string.
    String(String),
    /// Boolean.
    Bool(bool),
}

impl PartialEq for JScalarValue {
    fn eq(&self, other: &Self) -> bool {
        scalar_to_jsahibon(self) == scalar_to_jsahibon(other)
    }
}

/// Comparison operator used by scalar JSON predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum JCompareOp {
    /// `field == operand`.
    Eq,
    /// `field != operand`.
    Neq,
    /// `field > operand`.
    Gt,
    /// `field >= operand`.
    Gte,
    /// `field < operand`.
    Lt,
    /// `field <= operand`.
    Lte,
}

/// Polarity of an `in_` membership predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum JInPolarity {
    /// `field IN (operands…)`.
    In,
    /// `field NOT IN (operands…)`.
    NotIn,
}

/// Inspectable body of a JSON predicate stored under [`LookupOp::Json`].
///
/// Walkers downcast the [`FieldPredicate`] operand value via
/// [`FieldPredicate::value_as`](crate::predicate::FieldPredicate::value_as)
/// against this type, then pattern-match on the variant. Marked
/// `#[non_exhaustive]` so future variants can be added without breaking
/// downstream matchers.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum JSahibONPredicateBody {
    /// True when the path resolves to a value (any JSON kind, including
    /// `null`).
    Exists {
        /// JSON path the predicate was built against.
        path: JPath,
    },
    /// True when the path does not resolve.
    Missing {
        /// JSON path the predicate was built against.
        path: JPath,
    },
    /// True only when the resolved value exists and is JSON `null`.
    IsJsonNull {
        /// JSON path the predicate was built against.
        path: JPath,
    },
    /// True only when the resolved value exists and is not JSON `null`.
    IsNotJsonNull {
        /// JSON path the predicate was built against.
        path: JPath,
    },
    /// True when the resolved value matches the requested JSON value-type.
    Type {
        /// JSON path the predicate was built against.
        path: JPath,
        /// JSON kind to match against.
        kind: JTypeKind,
    },
    /// True when the resolved value is an object containing `key`.
    HasKey {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Object key required to be present.
        key: Arc<String>,
    },
    /// True when the resolved value is an object containing at least one of
    /// `keys`.
    HasAnyKey {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Object keys; the predicate is true when any one is present.
        keys: Arc<[String]>,
    },
    /// True when the resolved value is an object containing every key in
    /// `keys`.
    HasAllKeys {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Object keys; the predicate is true when all are present.
        keys: Arc<[String]>,
    },
    /// Compare the resolved scalar to a single operand under the requested
    /// scalar kind.
    ScalarCompare {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Comparison operator.
        op: JCompareOp,
        /// Scalar kind requested by the typed builder; the resolved value
        /// must match this kind (with cross-numeric softening for numbers).
        scalar_kind: JScalarKind,
        /// Operand value to compare against.
        operand: JScalarValue,
    },
    /// Test the resolved scalar against a membership set.
    ScalarIn {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Scalar kind requested by the typed builder.
        scalar_kind: JScalarKind,
        /// Operands forming the membership set.
        operands: Arc<[JScalarValue]>,
        /// In or NotIn polarity.
        polarity: JInPolarity,
    },
    /// Test that the resolved scalar lies in the inclusive range
    /// `[low, high]`.
    ScalarBetween {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Scalar kind requested by the typed builder.
        scalar_kind: JScalarKind,
        /// Inclusive lower bound.
        low: JScalarValue,
        /// Inclusive upper bound.
        high: JScalarValue,
    },
    /// True when the resolved JSON value structurally equals `value` under
    /// [`JSahibON`]'s manual equality.
    JsonEq {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Expected JSON value.
        value: JSahibON,
    },
    /// True when the resolved JSON value differs from `value` under
    /// [`JSahibON`] manual equality.
    JsonNeq {
        /// JSON path the predicate was built against.
        path: JPath,
        /// JSON value the resolved value must differ from.
        value: JSahibON,
    },
    /// True when the resolved JSON value is an array containing `element`
    /// under [`JSahibON`] manual equality.
    ArrayContains {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Element value the array must contain.
        element: JSahibON,
    },
    /// Compare the resolved JSON array's length against `len`.
    ArrayLen {
        /// JSON path the predicate was built against.
        path: JPath,
        /// Comparison operator.
        op: JCompareOp,
        /// Length operand. `usize` lengths are widened to `u64` at
        /// construction time.
        len: u64,
    },
}

/// Sealed marker trait for scalar operand types accepted by
/// [`JSahibONValueRef`].
///
/// V1 has exactly five impls: `i64`, `u64`, `f64`, `String`, and `bool`.
/// Narrow numeric widths (`i32`, `u16`, etc.) are widened at the call site.
pub trait JScalar: private::Sealed + Send + Sync + 'static {
    /// The scalar kind discriminant for this type.
    const KIND: JScalarKind;

    /// Convert the typed scalar into the type-erased
    /// [`JScalarValue`] operand.
    ///
    /// # Panics
    ///
    /// The `f64` impl panics when the input is NaN, `+Infinity`, or
    /// `-Infinity`; non-finite operands are rejected at predicate
    /// construction. Other impls (`i64`, `u64`, `String`, `bool`) never
    /// panic.
    fn into_scalar_value(self) -> JScalarValue;
}

/// Sealed marker trait for [`JScalar`] types that also support ordering
/// predicates (`gt`, `gte`, `lt`, `lte`, `between`).
///
/// V1 impls: `i64`, `u64`, `f64`. String ordering is intentionally absent in
/// v1 — locale collation is out of scope.
pub trait JOrderedScalar: JScalar {}

mod private {
    pub trait Sealed {}
}

impl private::Sealed for i64 {}
impl JScalar for i64 {
    const KIND: JScalarKind = JScalarKind::I64;
    fn into_scalar_value(self) -> JScalarValue {
        JScalarValue::I64(self)
    }
}
impl JOrderedScalar for i64 {}

impl private::Sealed for u64 {}
impl JScalar for u64 {
    const KIND: JScalarKind = JScalarKind::U64;
    fn into_scalar_value(self) -> JScalarValue {
        JScalarValue::U64(self)
    }
}
impl JOrderedScalar for u64 {}

impl private::Sealed for f64 {}
impl JScalar for f64 {
    const KIND: JScalarKind = JScalarKind::F64;

    /// Convert an `f64` into a [`JScalarValue::F64`].
    ///
    /// # Panics
    ///
    /// Panics when `self` is `NaN`, `+Infinity`, or `-Infinity`. Per
    /// [`JSahibON`]'s finite-floats invariant, non-finite
    /// values cannot be carried in the value model and so cannot meaningfully
    /// participate in a scalar predicate. Treat the panic as construction-time
    /// rejection: validate the operand before chaining `.gte(...)` / `.eq(...)`
    /// when it might be non-finite.
    fn into_scalar_value(self) -> JScalarValue {
        JScalarValue::F64(
            crate::JFiniteF64::try_new(self)
                .expect("JSahibON scalar predicates only accept finite f64 operands"),
        )
    }
}
impl JOrderedScalar for f64 {}

impl private::Sealed for String {}
impl JScalar for String {
    const KIND: JScalarKind = JScalarKind::String;
    fn into_scalar_value(self) -> JScalarValue {
        JScalarValue::String(self)
    }
}

impl private::Sealed for bool {}
impl JScalar for bool {
    const KIND: JScalarKind = JScalarKind::Bool;
    fn into_scalar_value(self) -> JScalarValue {
        JScalarValue::Bool(self)
    }
}

enum JRoot<T> {
    Required(fn(&T) -> &JSahibON),
    Optional(fn(&T) -> &Option<JSahibON>),
}

impl<T> Copy for JRoot<T> {}

impl<T> Clone for JRoot<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> JRoot<T> {
    fn resolve(self, value: &T) -> Option<&JSahibON> {
        match self {
            Self::Required(extract) => Some(extract(value)),
            Self::Optional(extract) => extract(value).as_ref(),
        }
    }
}

/// Builder anchored at a specific JSON path within a [`JSahibON`] cache field.
///
/// Path refs are produced by `Field<T, JSahibON>::jsahibon().path(...)` /
/// `key(...)` / `path_segments(...)` (and their `Option<JSahibON>` siblings)
/// and carry the predicate-construction surface for the resolved value.
pub struct JSahibONPathRef<T> {
    field_name: &'static str,
    root: JRoot<T>,
    path: JPath,
}

/// Field-level JSON predicate builder for `Field<T, JSahibON>`.
///
/// Constructed via [`Field<T, JSahibON>::jsahibon`]. Re-exposes the same
/// predicate surface as [`JSahibONPathRef`] anchored at the field root, plus
/// path-walking entry points (`path`, `key`, `path_segments`).
pub struct JSahibONFieldRef<T> {
    inner: JSahibONPathRef<T>,
}

/// Field-level JSON predicate builder for `Field<T, Option<JSahibON>>`.
///
/// Constructed via [`Field<T, Option<JSahibON>>::jsahibon`]. Same surface as
/// [`JSahibONFieldRef`], but `exists` / `missing` distinguish `None` (missing)
/// from `Some(JSahibON::Null)` (present, JSON `null`).
pub struct JSahibONOptionFieldRef<T> {
    inner: JSahibONPathRef<T>,
}

/// Typed scalar comparison builder produced by
/// [`JSahibONPathRef::value`] / [`JSahibONFieldRef::value`] /
/// [`JSahibONOptionFieldRef::value`].
///
/// The type parameter `V` must implement [`JScalar`] (or [`JOrderedScalar`]
/// for ordering methods).
pub struct JSahibONValueRef<T, V> {
    inner: JSahibONPathRef<T>,
    _marker: PhantomData<V>,
}

impl<T> Clone for JSahibONPathRef<T> {
    fn clone(&self) -> Self {
        Self {
            field_name: self.field_name,
            root: self.root,
            path: self.path.clone(),
        }
    }
}

impl<T> Clone for JSahibONFieldRef<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Clone for JSahibONOptionFieldRef<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, V> Clone for JSahibONValueRef<T, V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: 'static> Field<T, JSahibON> {
    /// Build a JSON predicate over a required `JSahibON` field.
    ///
    /// At root, `exists()` is always true and `missing()` is always false
    /// because the field cannot be absent. Use [`JSahibONFieldRef::path`] /
    /// [`JSahibONFieldRef::key`] / [`JSahibONFieldRef::path_segments`] to
    /// navigate into the value.
    pub fn jsahibon(&self) -> JSahibONFieldRef<T> {
        JSahibONFieldRef {
            inner: JSahibONPathRef {
                field_name: self.name,
                root: JRoot::Required(self.extract),
                path: JPath::root(),
            },
        }
    }
}

impl<T: 'static> Field<T, Option<JSahibON>> {
    /// Build a JSON predicate over an optional `JSahibON` field.
    ///
    /// `exists()` is true only for `Some(_)`; `missing()` is true only for
    /// `None`. `Some(JSahibON::Null)` exists and is JSON `null`.
    pub fn jsahibon(&self) -> JSahibONOptionFieldRef<T> {
        JSahibONOptionFieldRef {
            inner: JSahibONPathRef {
                field_name: self.name,
                root: JRoot::Optional(self.extract),
                path: JPath::root(),
            },
        }
    }
}

macro_rules! delegate_json_ref_methods {
    ($ty:ident) => {
        impl<T: 'static> $ty<T> {
            /// Return a path ref anchored at the JSON field root.
            pub fn root(&self) -> JSahibONPathRef<T> {
                let mut inner = self.inner.clone();
                inner.path = JPath::root();
                inner
            }

            /// Return a path ref for a dotted plain-identifier path.
            ///
            /// Panics when any segment is not a plain identifier; use
            /// [`Self::key`] or [`Self::path_segments`] for arbitrary keys.
            pub fn path(&self, dotted_plain_idents: &'static str) -> JSahibONPathRef<T> {
                let mut inner = self.inner.clone();
                inner.path = JPath::parse_dotted(dotted_plain_idents);
                inner
            }

            /// Return a path ref for a literal object key below the root.
            pub fn key(&self, key: impl Into<String>) -> JSahibONPathRef<T> {
                let mut inner = self.inner.clone();
                inner.path = inner.path.push(key.into());
                inner
            }

            /// Return a path ref from literal object-key segments.
            pub fn path_segments<I, S>(&self, segments: I) -> JSahibONPathRef<T>
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
            {
                let mut inner = self.inner.clone();
                inner.path = JPath::from_segments(segments);
                inner
            }

            /// Predicate that is true when the path resolves to any JSON value.
            pub fn exists(&self) -> BasicPredicate<T> {
                self.inner.exists()
            }

            /// Predicate that is true when the path does not resolve.
            pub fn missing(&self) -> BasicPredicate<T> {
                self.inner.missing()
            }

            /// Predicate that is true when the path resolves to JSON `null`.
            pub fn is_json_null(&self) -> BasicPredicate<T> {
                self.inner.is_json_null()
            }

            /// Predicate that is true when the path resolves to a non-null JSON value.
            pub fn is_not_json_null(&self) -> BasicPredicate<T> {
                self.inner.is_not_json_null()
            }

            /// Predicate that is true when the resolved value matches `kind`.
            pub fn is_type(&self, kind: JTypeKind) -> BasicPredicate<T> {
                self.inner.is_type(kind)
            }

            /// Shorthand for `is_type(JTypeKind::Bool)`.
            pub fn is_bool(&self) -> BasicPredicate<T> {
                self.inner.is_bool()
            }

            /// Shorthand for `is_type(JTypeKind::Number)`.
            pub fn is_number(&self) -> BasicPredicate<T> {
                self.inner.is_number()
            }

            /// Shorthand for `is_type(JTypeKind::String)`.
            pub fn is_string(&self) -> BasicPredicate<T> {
                self.inner.is_string()
            }

            /// Shorthand for `is_type(JTypeKind::Array)`.
            pub fn is_array(&self) -> BasicPredicate<T> {
                self.inner.is_array()
            }

            /// Shorthand for `is_type(JTypeKind::Object)`.
            pub fn is_object(&self) -> BasicPredicate<T> {
                self.inner.is_object()
            }

            /// Predicate that is true when the resolved object contains `key`.
            pub fn has_key(&self, key: impl Into<String>) -> BasicPredicate<T> {
                self.inner.has_key(key)
            }

            /// Predicate that is true when the resolved object contains any key.
            pub fn has_any_key<I, S>(&self, keys: I) -> BasicPredicate<T>
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
            {
                self.inner.has_any_key(keys)
            }

            /// Predicate that is true when the resolved object contains all keys.
            pub fn has_all_keys<I, S>(&self, keys: I) -> BasicPredicate<T>
            where
                I: IntoIterator<Item = S>,
                S: Into<String>,
            {
                self.inner.has_all_keys(keys)
            }

            /// Begin a typed scalar comparison against the resolved value.
            pub fn value<V: JScalar>(&self) -> JSahibONValueRef<T, V> {
                self.inner.value()
            }

            /// Predicate that is true when the resolved JSON value equals `value`.
            pub fn eq_json(&self, value: JSahibON) -> BasicPredicate<T> {
                self.inner.eq_json(value)
            }

            /// Predicate that is true when the resolved JSON value differs from `value`.
            pub fn neq_json(&self, value: JSahibON) -> BasicPredicate<T> {
                self.inner.neq_json(value)
            }

            /// Predicate that is true when the resolved array contains `element`.
            pub fn array_contains(&self, element: JSahibON) -> BasicPredicate<T> {
                self.inner.array_contains(element)
            }

            /// Predicate that is true when the resolved array length equals `len`.
            pub fn array_len_eq(&self, len: usize) -> BasicPredicate<T> {
                self.inner.array_len_eq(len)
            }

            /// Predicate that is true when the resolved array length is greater than `len`.
            pub fn array_len_gt(&self, len: usize) -> BasicPredicate<T> {
                self.inner.array_len_gt(len)
            }

            /// Predicate that is true when the resolved array length is at least `len`.
            pub fn array_len_gte(&self, len: usize) -> BasicPredicate<T> {
                self.inner.array_len_gte(len)
            }

            /// Predicate that is true when the resolved array length is less than `len`.
            pub fn array_len_lt(&self, len: usize) -> BasicPredicate<T> {
                self.inner.array_len_lt(len)
            }

            /// Predicate that is true when the resolved array length is at most `len`.
            pub fn array_len_lte(&self, len: usize) -> BasicPredicate<T> {
                self.inner.array_len_lte(len)
            }
        }
    };
}

delegate_json_ref_methods!(JSahibONFieldRef);
delegate_json_ref_methods!(JSahibONOptionFieldRef);

impl<T: 'static> JSahibONPathRef<T> {
    /// Push an additional literal object key onto this path.
    ///
    /// The key is taken verbatim (never parsed) so dots, hyphens, digits,
    /// empty strings, and non-ASCII text are addressed correctly.
    pub fn key(self, key: impl Into<String>) -> Self {
        Self {
            path: self.path.push(key.into()),
            ..self
        }
    }

    /// Append additional literal segments onto this path.
    pub fn path_segments<I, S>(self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            path: self.path.extend(segments),
            ..self
        }
    }

    /// Predicate that is true when the path resolves to a value (any JSON
    /// kind, including `null`).
    pub fn exists(&self) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::Exists {
            path: self.path.clone(),
        })
    }

    /// Predicate that is true when the path does not resolve.
    pub fn missing(&self) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::Missing {
            path: self.path.clone(),
        })
    }

    /// Predicate that is true only when the resolved value exists and is
    /// JSON `null`.
    pub fn is_json_null(&self) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::IsJsonNull {
            path: self.path.clone(),
        })
    }

    /// Predicate that is true only when the resolved value exists and is
    /// not JSON `null`.
    pub fn is_not_json_null(&self) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::IsNotJsonNull {
            path: self.path.clone(),
        })
    }

    /// Predicate that is true only when the resolved value matches `kind`.
    pub fn is_type(&self, kind: JTypeKind) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::Type {
            path: self.path.clone(),
            kind,
        })
    }

    /// Shorthand for `is_type(JTypeKind::Bool)`.
    pub fn is_bool(&self) -> BasicPredicate<T> {
        self.is_type(JTypeKind::Bool)
    }

    /// Shorthand for `is_type(JTypeKind::Number)`. Matches any of the three
    /// numeric carriers (`I64`, `U64`, `F64`).
    pub fn is_number(&self) -> BasicPredicate<T> {
        self.is_type(JTypeKind::Number)
    }

    /// Shorthand for `is_type(JTypeKind::String)`.
    pub fn is_string(&self) -> BasicPredicate<T> {
        self.is_type(JTypeKind::String)
    }

    /// Shorthand for `is_type(JTypeKind::Array)`.
    pub fn is_array(&self) -> BasicPredicate<T> {
        self.is_type(JTypeKind::Array)
    }

    /// Shorthand for `is_type(JTypeKind::Object)`.
    pub fn is_object(&self) -> BasicPredicate<T> {
        self.is_type(JTypeKind::Object)
    }

    /// Predicate that is true when the resolved value is an object
    /// containing `key`.
    pub fn has_key(&self, key: impl Into<String>) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::HasKey {
            path: self.path.clone(),
            key: Arc::new(key.into()),
        })
    }

    /// Predicate that is true when the resolved value is an object
    /// containing at least one of `keys`.
    pub fn has_any_key<I, S>(&self, keys: I) -> BasicPredicate<T>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.predicate(JSahibONPredicateBody::HasAnyKey {
            path: self.path.clone(),
            keys: keys.into_iter().map(Into::into).collect::<Vec<_>>().into(),
        })
    }

    /// Predicate that is true when the resolved value is an object
    /// containing every key in `keys`.
    pub fn has_all_keys<I, S>(&self, keys: I) -> BasicPredicate<T>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.predicate(JSahibONPredicateBody::HasAllKeys {
            path: self.path.clone(),
            keys: keys.into_iter().map(Into::into).collect::<Vec<_>>().into(),
        })
    }

    /// Begin a typed scalar comparison against the resolved value.
    ///
    /// Numeric scalar kinds (`i64`, `u64`, `f64`) accept any of the three
    /// JSON numeric carriers and compare in the portable numeric domain.
    /// `String` and `bool` are exact-kind matches and have no implicit
    /// coercions.
    pub fn value<V: JScalar>(&self) -> JSahibONValueRef<T, V> {
        JSahibONValueRef {
            inner: self.clone(),
            _marker: PhantomData,
        }
    }

    /// Predicate that is true when the resolved JSON value structurally
    /// equals `value` under [`JSahibON`]'s manual equality (objects are
    /// order-insensitive; numbers softened across `I64`/`U64`/`F64`).
    pub fn eq_json(&self, value: JSahibON) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::JsonEq {
            path: self.path.clone(),
            value,
        })
    }

    /// Predicate that is true when the resolved JSON value differs from
    /// `value` under [`JSahibON`] manual equality.
    pub fn neq_json(&self, value: JSahibON) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::JsonNeq {
            path: self.path.clone(),
            value,
        })
    }

    /// Predicate that is true when the resolved value is an array containing
    /// `element` under [`JSahibON`] manual equality.
    pub fn array_contains(&self, element: JSahibON) -> BasicPredicate<T> {
        self.predicate(JSahibONPredicateBody::ArrayContains {
            path: self.path.clone(),
            element,
        })
    }

    /// Predicate that is true when the resolved array's length equals `len`.
    pub fn array_len_eq(&self, len: usize) -> BasicPredicate<T> {
        self.array_len(JCompareOp::Eq, len)
    }

    /// Predicate that is true when the resolved array's length is greater
    /// than `len`.
    pub fn array_len_gt(&self, len: usize) -> BasicPredicate<T> {
        self.array_len(JCompareOp::Gt, len)
    }

    /// Predicate that is true when the resolved array's length is greater
    /// than or equal to `len`.
    pub fn array_len_gte(&self, len: usize) -> BasicPredicate<T> {
        self.array_len(JCompareOp::Gte, len)
    }

    /// Predicate that is true when the resolved array's length is less
    /// than `len`.
    pub fn array_len_lt(&self, len: usize) -> BasicPredicate<T> {
        self.array_len(JCompareOp::Lt, len)
    }

    /// Predicate that is true when the resolved array's length is less
    /// than or equal to `len`.
    pub fn array_len_lte(&self, len: usize) -> BasicPredicate<T> {
        self.array_len(JCompareOp::Lte, len)
    }

    fn array_len(&self, op: JCompareOp, len: usize) -> BasicPredicate<T> {
        let len = u64::try_from(len).expect("array length predicate exceeds u64");
        self.predicate(JSahibONPredicateBody::ArrayLen {
            path: self.path.clone(),
            op,
            len,
        })
    }

    fn predicate(&self, body: JSahibONPredicateBody) -> BasicPredicate<T> {
        let root = self.root;
        let body: Arc<JSahibONPredicateBody> = Arc::new(body);
        let body_for_eval = body.clone();
        let value: Arc<dyn Any + Send + Sync> = body;
        BasicPredicate::Field(FieldPredicate::new(
            self.field_name,
            LookupOp::Json,
            value,
            move |entry| evaluate_jsahibon_predicate(root.resolve(entry), body_for_eval.as_ref()),
        ))
    }
}

impl<T: 'static, V: JScalar> JSahibONValueRef<T, V> {
    /// `value == operand`. Type-mismatched resolved values evaluate to
    /// `false`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if `value` is `NaN`, `+Infinity`, or
    /// `-Infinity` per [`JScalar::into_scalar_value`].
    pub fn eq(&self, value: V) -> BasicPredicate<T> {
        self.compare(JCompareOp::Eq, value)
    }

    /// `value != operand`. Type-mismatched resolved values evaluate to
    /// `false`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if `value` is non-finite.
    pub fn neq(&self, value: V) -> BasicPredicate<T> {
        self.compare(JCompareOp::Neq, value)
    }

    /// `value IN (values…)`. Resolution failure or scalar-kind mismatch
    /// evaluates to `false`. An empty `values` slice with a present
    /// matching scalar evaluates to `false`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if any element of `values` is non-finite.
    pub fn in_(&self, values: Vec<V>) -> BasicPredicate<T> {
        self.in_predicate(values, JInPolarity::In)
    }

    /// `value NOT IN (values…)`. Resolution failure or scalar-kind mismatch
    /// evaluates to `false`. An empty `values` slice with a present
    /// matching scalar evaluates to `true`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if any element of `values` is non-finite.
    pub fn not_in(&self, values: Vec<V>) -> BasicPredicate<T> {
        self.in_predicate(values, JInPolarity::NotIn)
    }

    fn compare(&self, op: JCompareOp, value: V) -> BasicPredicate<T> {
        self.inner.predicate(JSahibONPredicateBody::ScalarCompare {
            path: self.inner.path.clone(),
            op,
            scalar_kind: V::KIND,
            operand: value.into_scalar_value(),
        })
    }

    fn in_predicate(&self, values: Vec<V>, polarity: JInPolarity) -> BasicPredicate<T> {
        self.inner.predicate(JSahibONPredicateBody::ScalarIn {
            path: self.inner.path.clone(),
            scalar_kind: V::KIND,
            operands: values
                .into_iter()
                .map(JScalar::into_scalar_value)
                .collect::<Vec<_>>()
                .into(),
            polarity,
        })
    }
}

impl<T: 'static, V: JOrderedScalar> JSahibONValueRef<T, V> {
    /// `value > operand`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if `value` is non-finite.
    pub fn gt(&self, value: V) -> BasicPredicate<T> {
        self.compare(JCompareOp::Gt, value)
    }

    /// `value >= operand`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if `value` is non-finite.
    pub fn gte(&self, value: V) -> BasicPredicate<T> {
        self.compare(JCompareOp::Gte, value)
    }

    /// `value < operand`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if `value` is non-finite.
    pub fn lt(&self, value: V) -> BasicPredicate<T> {
        self.compare(JCompareOp::Lt, value)
    }

    /// `value <= operand`.
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if `value` is non-finite.
    pub fn lte(&self, value: V) -> BasicPredicate<T> {
        self.compare(JCompareOp::Lte, value)
    }

    /// `low <= value <= high` (inclusive on both ends).
    ///
    /// # Panics
    ///
    /// When `V = f64`, panics if either `low` or `high` is non-finite.
    pub fn between(&self, low: V, high: V) -> BasicPredicate<T> {
        self.inner.predicate(JSahibONPredicateBody::ScalarBetween {
            path: self.inner.path.clone(),
            scalar_kind: V::KIND,
            low: low.into_scalar_value(),
            high: high.into_scalar_value(),
        })
    }
}

/// Evaluate a [`JSahibONPredicateBody`] against an in-memory JSON value.
///
/// `root` is the entry's root JSON value (`None` represents an absent
/// `Option<JSahibON>` field; `Some(&value)` represents a present value).
/// Returns the boolean result per the truth rules documented on
/// [`JSahibONPredicateBody`].
///
/// Downstream crates may reuse this evaluator instead of reimplementing
/// the truth rules — Sassi's `Punnu` evaluator already calls into it via
/// the predicate closure captured at construction.
pub fn evaluate_jsahibon_predicate(root: Option<&JSahibON>, body: &JSahibONPredicateBody) -> bool {
    match body {
        JSahibONPredicateBody::Exists { path } => resolve_path(root, path).is_some(),
        JSahibONPredicateBody::Missing { path } => resolve_path(root, path).is_none(),
        JSahibONPredicateBody::IsJsonNull { path } => {
            matches!(resolve_path(root, path), Some(JSahibON::Null))
        }
        JSahibONPredicateBody::IsNotJsonNull { path } => {
            resolve_path(root, path).is_some_and(|value| !matches!(value, JSahibON::Null))
        }
        JSahibONPredicateBody::Type { path, kind } => {
            resolve_path(root, path).is_some_and(|value| matches_type(value, *kind))
        }
        JSahibONPredicateBody::HasKey { path, key } => {
            object_at(root, path).is_some_and(|object| object.get(key.as_str()).is_some())
        }
        JSahibONPredicateBody::HasAnyKey { path, keys } => object_at(root, path)
            .is_some_and(|object| keys.iter().any(|key| object.get(key.as_str()).is_some())),
        JSahibONPredicateBody::HasAllKeys { path, keys } => object_at(root, path)
            .is_some_and(|object| keys.iter().all(|key| object.get(key.as_str()).is_some())),
        JSahibONPredicateBody::ScalarCompare {
            path,
            op,
            scalar_kind,
            operand,
        } => scalar_at(root, path, *scalar_kind)
            .is_some_and(|left| compare_scalar(&left, *op, operand)),
        JSahibONPredicateBody::ScalarIn {
            path,
            scalar_kind,
            operands,
            polarity,
        } => scalar_at(root, path, *scalar_kind).is_some_and(|left| {
            let contains = operands.iter().any(|right| &left == right);
            match polarity {
                JInPolarity::In => contains,
                JInPolarity::NotIn => !contains,
            }
        }),
        JSahibONPredicateBody::ScalarBetween {
            path,
            scalar_kind,
            low,
            high,
        } => scalar_at(root, path, *scalar_kind).is_some_and(|left| {
            compare_scalar_order(&left, low).is_some_and(|ordering| ordering != Ordering::Less)
                && compare_scalar_order(&left, high)
                    .is_some_and(|ordering| ordering != Ordering::Greater)
        }),
        JSahibONPredicateBody::JsonEq { path, value } => {
            resolve_path(root, path).is_some_and(|left| left == value)
        }
        JSahibONPredicateBody::JsonNeq { path, value } => {
            resolve_path(root, path).is_some_and(|left| left != value)
        }
        JSahibONPredicateBody::ArrayContains { path, element } => match resolve_path(root, path) {
            Some(JSahibON::Array(values)) => values.iter().any(|value| value == element),
            _ => false,
        },
        JSahibONPredicateBody::ArrayLen { path, op, len } => match resolve_path(root, path) {
            Some(JSahibON::Array(values)) => {
                compare_u64(u64::try_from(values.len()).unwrap_or(u64::MAX), *op, *len)
            }
            _ => false,
        },
    }
}

fn matches_type(value: &JSahibON, kind: JTypeKind) -> bool {
    matches!(
        (value, kind),
        (JSahibON::Null, JTypeKind::Null)
            | (JSahibON::Bool(_), JTypeKind::Bool)
            | (
                JSahibON::I64(_) | JSahibON::U64(_) | JSahibON::F64(_),
                JTypeKind::Number
            )
            | (JSahibON::String(_), JTypeKind::String)
            | (JSahibON::Array(_), JTypeKind::Array)
            | (JSahibON::Object(_), JTypeKind::Object)
    )
}

fn resolve_path<'a>(root: Option<&'a JSahibON>, path: &JPath) -> Option<&'a JSahibON> {
    let mut current = root?;
    for segment in path.segments() {
        let JSahibON::Object(object) = current else {
            return None;
        };
        current = object.get(segment)?;
    }
    Some(current)
}

fn object_at<'a>(root: Option<&'a JSahibON>, path: &JPath) -> Option<&'a JObject> {
    match resolve_path(root, path) {
        Some(JSahibON::Object(object)) => Some(object),
        _ => None,
    }
}

fn scalar_at(root: Option<&JSahibON>, path: &JPath, kind: JScalarKind) -> Option<JScalarValue> {
    let value = resolve_path(root, path)?;
    match (kind, value) {
        (JScalarKind::I64 | JScalarKind::U64 | JScalarKind::F64, JSahibON::I64(value)) => {
            Some(JScalarValue::I64(*value))
        }
        (JScalarKind::I64 | JScalarKind::U64 | JScalarKind::F64, JSahibON::U64(value)) => {
            Some(JScalarValue::U64(*value))
        }
        (JScalarKind::I64 | JScalarKind::U64 | JScalarKind::F64, JSahibON::F64(value)) => {
            Some(JScalarValue::F64(*value))
        }
        (JScalarKind::String, JSahibON::String(value)) => Some(JScalarValue::String(value.clone())),
        (JScalarKind::Bool, JSahibON::Bool(value)) => Some(JScalarValue::Bool(*value)),
        _ => None,
    }
}

fn compare_scalar(left: &JScalarValue, op: JCompareOp, right: &JScalarValue) -> bool {
    match op {
        JCompareOp::Eq => left == right,
        JCompareOp::Neq => left != right,
        JCompareOp::Gt => {
            compare_scalar_order(left, right).is_some_and(|ordering| ordering == Ordering::Greater)
        }
        JCompareOp::Gte => {
            compare_scalar_order(left, right).is_some_and(|ordering| ordering != Ordering::Less)
        }
        JCompareOp::Lt => {
            compare_scalar_order(left, right).is_some_and(|ordering| ordering == Ordering::Less)
        }
        JCompareOp::Lte => {
            compare_scalar_order(left, right).is_some_and(|ordering| ordering != Ordering::Greater)
        }
    }
}

fn compare_scalar_order(left: &JScalarValue, right: &JScalarValue) -> Option<Ordering> {
    compare_jsahibon_numbers(&scalar_to_jsahibon(left), &scalar_to_jsahibon(right))
}

fn compare_u64(left: u64, op: JCompareOp, right: u64) -> bool {
    match op {
        JCompareOp::Eq => left == right,
        JCompareOp::Neq => left != right,
        JCompareOp::Gt => left > right,
        JCompareOp::Gte => left >= right,
        JCompareOp::Lt => left < right,
        JCompareOp::Lte => left <= right,
    }
}

fn scalar_to_jsahibon(value: &JScalarValue) -> JSahibON {
    match value {
        JScalarValue::I64(value) => JSahibON::I64(*value),
        JScalarValue::U64(value) => JSahibON::U64(*value),
        JScalarValue::F64(value) => JSahibON::F64(*value),
        JScalarValue::String(value) => JSahibON::String(value.clone()),
        JScalarValue::Bool(value) => JSahibON::Bool(*value),
    }
}
