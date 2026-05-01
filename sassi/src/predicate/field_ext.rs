//! Field extension methods — the predicate-builder surface on
//! [`Field<T, V>`](crate::cacheable::Field).
//!
//! Each method returns a [`BasicPredicate<T>`]. The methods are
//! organised by the trait bounds they require on `V`: `PartialEq` for
//! equality, `PartialOrd` for ordering, and string-specific methods
//! for `Field<T, String>`. Special methods exist for `Option<U>`
//! fields (`is_null` / `is_not_null`).
//!
//! Case-insensitive string ops use `str::eq_ignore_ascii_case` and
//! `to_ascii_lowercase().contains(...)` rather than a regex engine —
//! djogi's "no Rust regex" doctrine carries over here even though
//! sassi has no djogi dependency, because the rule reflects a sound
//! preference for stdlib primitives.

use super::basic::BasicPredicate;
use super::field_predicate::{FieldPredicate, LookupOp};
use crate::cacheable::Field;

// === Equality ============================================================

impl<T: 'static, V: PartialEq + Send + Sync + 'static> Field<T, V> {
    /// `field == value`.
    pub fn eq(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::Eq, move |t| {
            extract(t) == &val
        }))
    }

    /// `field != value`.
    pub fn neq(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::Neq, move |t| {
            extract(t) != &val
        }))
    }
}

// === Ordering ============================================================

impl<T: 'static, V: PartialOrd + Send + Sync + 'static> Field<T, V> {
    /// `field > value`.
    pub fn gt(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::Gt, move |t| {
            extract(t) > &val
        }))
    }

    /// `field >= value`.
    pub fn gte(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::Gte, move |t| {
            extract(t) >= &val
        }))
    }

    /// `field < value`.
    pub fn lt(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::Lt, move |t| {
            extract(t) < &val
        }))
    }

    /// `field <= value`.
    pub fn lte(&self, val: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::Lte, move |t| {
            extract(t) <= &val
        }))
    }

    /// `low <= field <= high` (inclusive on both ends).
    pub fn between(&self, low: V, high: V) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Between,
            move |t| {
                let v = extract(t);
                v >= &low && v <= &high
            },
        ))
    }
}

// === Membership ==========================================================

impl<T: 'static, V: PartialEq + Send + Sync + 'static> Field<T, V> {
    /// `field IN (values…)`.
    pub fn in_(&self, vals: Vec<V>) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::In, move |t| {
            let v = extract(t);
            vals.iter().any(|cand| cand == v)
        }))
    }

    /// `field NOT IN (values…)`.
    pub fn not_in(&self, vals: Vec<V>) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::NotIn, move |t| {
            let v = extract(t);
            !vals.iter().any(|cand| cand == v)
        }))
    }
}

// === Null tests for Option<U> fields =====================================

impl<T: 'static, U: Send + Sync + 'static> Field<T, Option<U>> {
    /// `field IS NULL`. Only meaningful on `Option<U>` fields.
    pub fn is_null(&self) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::IsNull, move |t| {
            extract(t).is_none()
        }))
    }

    /// `field IS NOT NULL`. Only meaningful on `Option<U>` fields.
    pub fn is_not_null(&self) -> BasicPredicate<T> {
        let extract = self.extract;
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IsNotNull,
            move |t| extract(t).is_some(),
        ))
    }
}

// === String-specific operations ==========================================

impl<T: 'static> Field<T, String> {
    /// Case-sensitive substring match: `field` contains the needle.
    pub fn contains(&self, needle: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let needle: String = needle.to_owned();
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::Contains,
            move |t| extract(t).contains(needle.as_str()),
        ))
    }

    /// Case-insensitive substring match (ASCII-fast-path; UTF-8
    /// preserved by `to_lowercase`).
    pub fn icontains(&self, needle: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let needle_lower: String = needle.to_lowercase();
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IContains,
            move |t| extract(t).to_lowercase().contains(needle_lower.as_str()),
        ))
    }

    /// `field` starts with the prefix (case-sensitive).
    pub fn starts_with(&self, prefix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let prefix: String = prefix.to_owned();
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::StartsWith,
            move |t| extract(t).starts_with(prefix.as_str()),
        ))
    }

    /// `field` starts with the prefix (case-insensitive).
    pub fn istarts_with(&self, prefix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let prefix_lower: String = prefix.to_lowercase();
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IStartsWith,
            move |t| extract(t).to_lowercase().starts_with(prefix_lower.as_str()),
        ))
    }

    /// `field` ends with the suffix (case-sensitive).
    pub fn ends_with(&self, suffix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let suffix: String = suffix.to_owned();
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::EndsWith,
            move |t| extract(t).ends_with(suffix.as_str()),
        ))
    }

    /// `field` ends with the suffix (case-insensitive).
    pub fn iends_with(&self, suffix: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let suffix_lower: String = suffix.to_lowercase();
        BasicPredicate::Field(FieldPredicate::new(
            self.name,
            LookupOp::IEndsWith,
            move |t| extract(t).to_lowercase().ends_with(suffix_lower.as_str()),
        ))
    }

    /// `field == value` (case-insensitive ASCII fast-path; falls back
    /// to full `to_lowercase` for non-ASCII inputs).
    pub fn iexact(&self, val: &str) -> BasicPredicate<T> {
        let extract = self.extract;
        let val: String = val.to_owned();
        BasicPredicate::Field(FieldPredicate::new(self.name, LookupOp::IExact, move |t| {
            let extracted = extract(t);
            if extracted.is_ascii() && val.is_ascii() {
                extracted.eq_ignore_ascii_case(val.as_str())
            } else {
                extracted.to_lowercase() == val.to_lowercase()
            }
        }))
    }
}
