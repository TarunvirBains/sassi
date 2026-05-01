//! [`FieldPredicate<T>`] — the per-field predicate payload carried inside
//! [`BasicPredicate::Field`](super::BasicPredicate::Field).
//!
//! Each `FieldPredicate` carries three pieces of information so it can
//! be walked by both in-memory evaluators (sassi's own `evaluate`) AND
//! external walkers (downstream SQL emitters in djogi, predicate-plan
//! debug formatters):
//!
//! 1. The **field name** (`field_name`) — the column / serde key.
//! 2. The **operator marker** (`op` of type [`LookupOp`]) — informational
//!    in sassi proper but load-bearing for SQL emitters that dispatch on
//!    op kind.
//! 3. The **operand value(s)** (`value`) — type-erased as
//!    `Arc<dyn Any + Send + Sync>`; downstream walkers downcast via
//!    [`std::any::Any::downcast_ref`] to inspect. Layout depends on
//!    `op` (see [`LookupOp`] documentation).
//!
//! Plus a pre-built evaluation closure (`eval`) for the fast-path
//! in-memory walk — captured at construction so `evaluate` never has
//! to re-dispatch on op + value type at runtime.

use std::any::Any;
use std::sync::Arc;

/// Lookup operator marker. Used for diagnostics + by SQL-emitting
/// downstream consumers (e.g., djogi's `Q<T>` walker) to choose the
/// right SQL construction. Also tells walkers what the
/// [`FieldPredicate::value`] payload contains:
///
/// | Op | `value` payload |
/// |---|---|
/// | `Eq`, `Neq`, `Gt`, `Gte`, `Lt`, `Lte` | `Arc<V>` |
/// | `In`, `NotIn` | `Arc<Vec<V>>` |
/// | `Between` | `Arc<(V, V)>` |
/// | `IsNull`, `IsNotNull` | `Arc<()>` (no operand) |
/// | `Contains`, `IContains`, `StartsWith`, `IStartsWith`, `EndsWith`, `IEndsWith`, `IExact` | `Arc<String>` |
///
/// Marked `#[non_exhaustive]` so adding new ops (e.g., `Like` for SQL
/// raw-pattern, `Regex`) in v0.2+ doesn't break downstream matchers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LookupOp {
    /// `field == value`
    Eq,
    /// `field != value`
    Neq,
    /// `field > value`
    Gt,
    /// `field >= value`
    Gte,
    /// `field < value`
    Lt,
    /// `field <= value`
    Lte,
    /// `field IN (values…)`
    In,
    /// `field NOT IN (values…)`
    NotIn,
    /// `field IS NULL` (only valid when the field type is `Option<U>`)
    IsNull,
    /// `field IS NOT NULL` (only valid when the field type is `Option<U>`)
    IsNotNull,
    /// `low <= field <= high`
    Between,
    /// `field` contains the substring (case-sensitive)
    Contains,
    /// `field` contains the substring (case-insensitive ASCII)
    IContains,
    /// `field` starts with the prefix (case-sensitive)
    StartsWith,
    /// `field` starts with the prefix (case-insensitive ASCII)
    IStartsWith,
    /// `field` ends with the suffix (case-sensitive)
    EndsWith,
    /// `field` ends with the suffix (case-insensitive ASCII)
    IEndsWith,
    /// `field == value` (case-insensitive ASCII)
    IExact,
}

/// Single-field predicate. Carries the field name, operator marker, the
/// type-erased operand value, and a pre-built evaluation closure.
///
/// Construction is via the `Field<T, V>` lookup methods (see
/// [`Field::eq`](crate::cacheable::Field), `gt`, `contains`, etc.). The
/// closure-capture pattern keeps `FieldPredicate<T>` `'static` (no
/// borrows of the construction-time value) and `Send + Sync` provided
/// the captured value is `Send + Sync`.
///
/// All fields are private. Use [`field_name`](Self::field_name),
/// [`op`](Self::op), [`value`](Self::value), and
/// [`value_as`](Self::value_as) to access; use
/// [`evaluate`](Self::evaluate) to apply.
pub struct FieldPredicate<T> {
    field_name: &'static str,
    op: LookupOp,
    value: Arc<dyn Any + Send + Sync>,
    eval: Arc<dyn Fn(&T) -> bool + Send + Sync>,
}

impl<T> Clone for FieldPredicate<T> {
    fn clone(&self) -> Self {
        Self {
            field_name: self.field_name,
            op: self.op,
            value: self.value.clone(),
            eval: self.eval.clone(),
        }
    }
}

impl<T> std::fmt::Debug for FieldPredicate<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldPredicate")
            .field("field", &self.field_name)
            .field("op", &self.op)
            .finish_non_exhaustive()
    }
}

impl<T> FieldPredicate<T> {
    /// Internal constructor — used by `Field<T, V>` lookup methods.
    pub(crate) fn new<F>(
        field_name: &'static str,
        op: LookupOp,
        value: Arc<dyn Any + Send + Sync>,
        eval: F,
    ) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self {
            field_name,
            op,
            value,
            eval: Arc::new(eval),
        }
    }

    /// Column / serde-key name.
    #[inline]
    pub fn field_name(&self) -> &'static str {
        self.field_name
    }

    /// Operator marker. Tells walkers what shape the [`value`](Self::value)
    /// payload carries (see [`LookupOp`] doc).
    #[inline]
    pub fn op(&self) -> LookupOp {
        self.op
    }

    /// Raw access to the type-erased operand value. Walkers that need
    /// to dispatch on `TypeId` or pass the value through serialization
    /// use this. For typed access, see [`value_as`](Self::value_as).
    #[inline]
    pub fn value(&self) -> &(dyn Any + Send + Sync) {
        &*self.value
    }

    /// Typed access to the operand value. Returns `Some(&V)` if the
    /// captured value's runtime type matches `V`, `None` otherwise.
    /// Layout depends on the op (see [`LookupOp`] doc) — for
    /// equality/comparison ops `V` is the field's value type; for
    /// `Between` it's `(V, V)`; for `In`/`NotIn` it's `Vec<V>`;
    /// for null tests it's `()`; for string ops it's `String`.
    #[inline]
    pub fn value_as<V: Any + 'static>(&self) -> Option<&V> {
        self.value.downcast_ref::<V>()
    }

    /// Evaluate the predicate against an in-memory `&T`. Uses the
    /// closure captured at construction; does not re-dispatch on op.
    #[inline]
    pub(crate) fn evaluate(&self, value: &T) -> bool {
        (self.eval)(value)
    }
}
