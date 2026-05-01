//! [`FieldPredicate<T>`] — the per-field predicate payload carried inside
//! [`BasicPredicate::Field`](super::BasicPredicate::Field).

use std::sync::Arc;

/// Lookup operator marker. Used for diagnostics + by SQL-emitting
/// downstream consumers (e.g., djogi's `Q<T>` walker) to choose the
/// right SQL construction.
///
/// In sassi proper, the operator marker is informational only — the
/// actual evaluation logic is captured in
/// [`FieldPredicate::eval`] as a closure, so reading the marker is
/// optional for the in-memory walker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Single-field predicate. Carries the field name (for diagnostics +
/// SQL emit by downstream consumers), the operator marker, and a
/// pre-built evaluation closure that captures the comparison value at
/// construction time.
///
/// Construction is via the `Field<T, V>` lookup methods (see
/// [`Field::eq`](crate::cacheable::Field), `gt`, `contains`, etc.). The
/// closure-capture pattern keeps `FieldPredicate<T>` `'static` (no
/// borrows of the construction-time value) and `Send + Sync` provided
/// the captured value is `Send + Sync`.
pub struct FieldPredicate<T> {
    /// Column / serde-key name of the field. Always equal to the
    /// originating [`Field::name`](crate::cacheable::Field::name).
    pub field_name: &'static str,

    /// Operator marker. Informational for sassi (the evaluation is
    /// closure-driven); load-bearing for SQL-emitting consumers.
    pub op: LookupOp,

    /// Pre-built evaluator. Closure captures the comparison value(s)
    /// at construction. Must be total (no panics) and deterministic.
    pub(crate) eval: Arc<dyn Fn(&T) -> bool + Send + Sync>,
}

impl<T> Clone for FieldPredicate<T> {
    fn clone(&self) -> Self {
        Self {
            field_name: self.field_name,
            op: self.op,
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
    pub(crate) fn new<F>(field_name: &'static str, op: LookupOp, eval: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self {
            field_name,
            op,
            eval: Arc::new(eval),
        }
    }
}
