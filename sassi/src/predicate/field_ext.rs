//! Field extension methods — the predicate-builder surface on
//! [`Field<T, V>`](crate::cacheable::Field).
//!
//! Each method returns a [`BasicPredicate<T>`]. The methods are
//! organised by the trait bounds they require on `V`: `PartialEq` for
//! equality, `PartialOrd` for ordering, and string-specific methods
//! for `Field<T, String>`. Special methods exist for `Option<U>`
//! fields (`is_null` / `is_not_null`).
//!
//! Each method also captures the operand value into the constructed
//! [`FieldPredicate`], type-erased as `Arc<dyn Any + Send + Sync>`,
//! so downstream walkers (SQL emitters, debug formatters) can
//! downcast and inspect. Layout per op is documented on [`LookupOp`].
//!
//! Case-insensitive string ops use ASCII-only case folding rather than
//! a regex engine or Unicode locale folding. This keeps in-memory
//! predicates portable across runtimes and lets downstream SQL emitters
//! mirror the same contract exactly.

use super::basic::BasicPredicate;
use super::field_predicate::{FieldPredicate, LookupOp};
use crate::cacheable::Field;
use std::any::Any;
use std::sync::Arc;

fn ascii_fold(value: &str) -> String {
    value.to_ascii_lowercase()
}

// === Equality ============================================================

impl<T: 'static, V: PartialEq + Send + Sync + 'static> Field<T, V> {
    /// `field == value`.
    pub fn eq(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Eq,
            value,
            move |t| extract(t) == &*val_for_eval,
        ))
    }

    /// `field != value`.
    pub fn neq(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Neq,
            value,
            move |t| extract(t) != &*val_for_eval,
        ))
    }
}

// === Ordering ============================================================

impl<T: 'static, V: PartialOrd + Send + Sync + 'static> Field<T, V> {
    /// `field > value`.
    pub fn gt(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Gt,
            value,
            move |t| extract(t) > &*val_for_eval,
        ))
    }

    /// `field >= value`.
    pub fn gte(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Gte,
            value,
            move |t| extract(t) >= &*val_for_eval,
        ))
    }

    /// `field < value`.
    pub fn lt(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Lt,
            value,
            move |t| extract(t) < &*val_for_eval,
        ))
    }

    /// `field <= value`.
    pub fn lte(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Lte,
            value,
            move |t| extract(t) <= &*val_for_eval,
        ))
    }

    /// `low <= field <= high` (inclusive on both ends).
    ///
    /// Operand value layout: `Arc<(V, V)>` carrying `(low, high)`.
    pub fn between(&self, low: V, high: V) -> BasicPredicate<T> {
        let extract = self.extract;
        let pair_arc: Arc<(V, V)> = Arc::new((low, high));
        let pair_for_eval = pair_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = pair_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Between,
            value,
            move |t| {
                let v = extract(t);
                v >= &pair_for_eval.0 && v <= &pair_for_eval.1
            },
        ))
    }
}

// === Membership ==========================================================

impl<T: 'static, V: PartialEq + Send + Sync + 'static> Field<T, V> {
    /// `field IN (values…)`. Operand value layout: `Arc<Vec<V>>`.
    pub fn in_(&self, vals: Vec<V>) -> BasicPredicate<T> {
        let extract = self.extract;
        let vec_arc: Arc<Vec<V>> = Arc::new(vals);
        let vec_for_eval = vec_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = vec_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::In,
            value,
            move |t| {
                let v = extract(t);
                vec_for_eval.iter().any(|cand| cand == v)
            },
        ))
    }

    /// `field NOT IN (values…)`. Operand value layout: `Arc<Vec<V>>`.
    pub fn not_in(&self, vals: Vec<V>) -> BasicPredicate<T> {
        let extract = self.extract;
        let vec_arc: Arc<Vec<V>> = Arc::new(vals);
        let vec_for_eval = vec_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = vec_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::NotIn,
            value,
            move |t| {
                let v = extract(t);
                !vec_for_eval.iter().any(|cand| cand == v)
            },
        ))
    }
}

// === Null tests for Option<U> fields =====================================

impl<T: 'static, U: Send + Sync + 'static> Field<T, Option<U>> {
    /// `field IS NULL`. Only meaningful on `Option<U>` fields.
    /// Operand value layout: `Arc<()>` (no operand).
    pub fn is_null(&self) -> BasicPredicate<T> {
        let extract = self.extract;
        let value: Arc<dyn Any + Send + Sync> = Arc::new(());
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IsNull,
            value,
            move |t| extract(t).is_none(),
        ))
    }

    /// `field IS NOT NULL`. Only meaningful on `Option<U>` fields.
    /// Operand value layout: `Arc<()>` (no operand).
    pub fn is_not_null(&self) -> BasicPredicate<T> {
        let extract = self.extract;
        let value: Arc<dyn Any + Send + Sync> = Arc::new(());
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IsNotNull,
            value,
            move |t| extract(t).is_some(),
        ))
    }
}

/// View of an optional field that only compares the present inner value.
///
/// `None` evaluates to `false` for every `PresentField` value comparison.
pub struct PresentField<T, V> {
    field: Field<T, Option<V>>,
}

impl<T, V> std::fmt::Debug for PresentField<T, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresentField")
            .field("name", &self.field.name)
            .finish_non_exhaustive()
    }
}

impl<T, V> Copy for PresentField<T, V> {}

impl<T, V> Clone for PresentField<T, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, V> Field<T, Option<V>> {
    /// Compare only the present `Some(V)` value.
    pub fn some(&self) -> PresentField<T, V> {
        PresentField { field: *self }
    }
}

impl<T: 'static, V: PartialEq + Send + Sync + 'static> PresentField<T, V> {
    /// `field IS NOT NULL AND field == value`.
    pub fn eq(&self, val: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Eq,
            value,
            move |t| extract(t).as_ref().is_some_and(|v| v == &*val_for_eval),
        ))
    }

    /// `field IS NOT NULL AND field != value`.
    pub fn neq(&self, val: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Neq,
            value,
            move |t| extract(t).as_ref().is_some_and(|v| v != &*val_for_eval),
        ))
    }

    /// `field IS NOT NULL AND field IN (values...)`.
    pub fn in_(&self, vals: Vec<V>) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let vec_arc: Arc<Vec<V>> = Arc::new(vals);
        let vec_for_eval = vec_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = vec_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::In,
            value,
            move |t| {
                extract(t)
                    .as_ref()
                    .is_some_and(|v| vec_for_eval.iter().any(|cand| cand == v))
            },
        ))
    }

    /// `field IS NOT NULL AND field NOT IN (values...)`.
    pub fn not_in(&self, vals: Vec<V>) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let vec_arc: Arc<Vec<V>> = Arc::new(vals);
        let vec_for_eval = vec_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = vec_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::NotIn,
            value,
            move |t| {
                extract(t)
                    .as_ref()
                    .is_some_and(|v| !vec_for_eval.iter().any(|cand| cand == v))
            },
        ))
    }
}

impl<T: 'static, V: PartialOrd + Send + Sync + 'static> PresentField<T, V> {
    /// `field IS NOT NULL AND field > value`.
    pub fn gt(&self, val: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Gt,
            value,
            move |t| extract(t).as_ref().is_some_and(|v| v > &*val_for_eval),
        ))
    }

    /// `field IS NOT NULL AND field >= value`.
    pub fn gte(&self, val: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Gte,
            value,
            move |t| extract(t).as_ref().is_some_and(|v| v >= &*val_for_eval),
        ))
    }

    /// `field IS NOT NULL AND field < value`.
    pub fn lt(&self, val: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Lt,
            value,
            move |t| extract(t).as_ref().is_some_and(|v| v < &*val_for_eval),
        ))
    }

    /// `field IS NOT NULL AND field <= value`.
    pub fn lte(&self, val: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let val_arc: Arc<V> = Arc::new(val);
        let val_for_eval = val_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Lte,
            value,
            move |t| extract(t).as_ref().is_some_and(|v| v <= &*val_for_eval),
        ))
    }

    /// `field IS NOT NULL AND low <= field <= high`.
    pub fn between(&self, low: V, high: V) -> BasicPredicate<T> {
        let extract = self.field.extract;
        let pair_arc: Arc<(V, V)> = Arc::new((low, high));
        let pair_for_eval = pair_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = pair_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.field.name,
            LookupOp::Between,
            value,
            move |t| {
                extract(t)
                    .as_ref()
                    .is_some_and(|v| v >= &pair_for_eval.0 && v <= &pair_for_eval.1)
            },
        ))
    }
}

// === String-specific operations ==========================================

impl<T: 'static> Field<T, String> {
    /// Case-sensitive substring match: `field` contains the needle.
    /// Operand value layout: `Arc<String>`.
    pub fn contains(&self, needle: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let needle_arc: Arc<String> = Arc::new(needle.to_owned());
        let needle_for_eval = needle_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = needle_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Contains,
            value,
            move |t| extract(t).contains(needle_for_eval.as_str()),
        ))
    }

    /// Case-insensitive substring match using ASCII-only folding.
    /// Operand value layout: `Arc<String>`
    /// — original (non-lowered) needle for inspection.
    pub fn icontains(&self, needle: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let needle_arc: Arc<String> = Arc::new(needle.to_owned());
        let needle_lower = ascii_fold(needle);
        let value: Arc<dyn Any + Send + Sync> = needle_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IContains,
            value,
            move |t| ascii_fold(extract(t)).contains(needle_lower.as_str()),
        ))
    }

    /// `field` starts with the prefix (case-sensitive). Operand value
    /// layout: `Arc<String>`.
    pub fn starts_with(&self, prefix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let prefix_arc: Arc<String> = Arc::new(prefix.to_owned());
        let prefix_for_eval = prefix_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = prefix_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::StartsWith,
            value,
            move |t| extract(t).starts_with(prefix_for_eval.as_str()),
        ))
    }

    /// `field` starts with the prefix (case-insensitive).
    /// Operand value layout: `Arc<String>` — original (non-lowered).
    pub fn istarts_with(&self, prefix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let prefix_arc: Arc<String> = Arc::new(prefix.to_owned());
        let prefix_lower = ascii_fold(prefix);
        let value: Arc<dyn Any + Send + Sync> = prefix_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IStartsWith,
            value,
            move |t| ascii_fold(extract(t)).starts_with(prefix_lower.as_str()),
        ))
    }

    /// `field` ends with the suffix (case-sensitive). Operand value
    /// layout: `Arc<String>`.
    pub fn ends_with(&self, suffix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let suffix_arc: Arc<String> = Arc::new(suffix.to_owned());
        let suffix_for_eval = suffix_arc.clone();
        let value: Arc<dyn Any + Send + Sync> = suffix_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::EndsWith,
            value,
            move |t| extract(t).ends_with(suffix_for_eval.as_str()),
        ))
    }

    /// `field` ends with the suffix (case-insensitive).
    /// Operand value layout: `Arc<String>` — original (non-lowered).
    pub fn iends_with(&self, suffix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let suffix_arc: Arc<String> = Arc::new(suffix.to_owned());
        let suffix_lower = ascii_fold(suffix);
        let value: Arc<dyn Any + Send + Sync> = suffix_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IEndsWith,
            value,
            move |t| ascii_fold(extract(t)).ends_with(suffix_lower.as_str()),
        ))
    }

    /// `field == value` using ASCII-only case folding.
    /// Operand value layout: `Arc<String>` — original.
    pub fn iexact(&self, val: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let val_arc: Arc<String> = Arc::new(val.to_owned());
        let val_folded = ascii_fold(val);
        let value: Arc<dyn Any + Send + Sync> = val_arc;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IExact,
            value,
            move |t| ascii_fold(extract(t)) == val_folded,
        ))
    }
}
