//! [`BasicPredicate<T>`] — the universal predicate algebra.
//!
//! Composes via `&` (And), `|` (Or), `^` (Xor), `!` (Not) with `And` /
//! `Or` flattening so chained `a & b & c` produces
//! `And(vec![a, b, c])` rather than a nested binary tree.
//! Evaluates against `&T` via [`BasicPredicate::evaluate`]; lowers to
//! SQL when consumed by a downstream emitter that pairs the operator
//! marker with the field name.

use super::field_predicate::FieldPredicate;
use std::ops::{BitAnd, BitOr, BitXor, Not};

/// Universal predicate algebra over `T`.
///
/// Marked `#[non_exhaustive]` to keep room for new variants (e.g.,
/// future `IfThenElse`) without semver-breaking downstream pattern
/// matches.
///
/// # Composition
///
/// ```
/// use sassi::{BasicPredicate, Field};
///
/// struct User { age: u32, banned: bool }
///
/// let age_field: Field<User, u32> = Field::new("age", |u| &u.age);
/// let banned_field: Field<User, bool> = Field::new("banned", |u| &u.banned);
///
/// let active_adult: BasicPredicate<User> = age_field.gte(18) & !banned_field.eq(true);
/// let alice = User { age: 30, banned: false };
/// assert!(active_adult.evaluate(&alice));
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BasicPredicate<T> {
    /// Always-true sentinel. Useful as an identity for `And` reductions.
    True,
    /// Always-false sentinel. Useful as an identity for `Or` reductions.
    False,
    /// Single-field predicate. Constructed via
    /// [`Field::eq`](crate::cacheable::Field) etc.
    Field(FieldPredicate<T>),
    /// Logical conjunction. Flattened: `a & b & c` produces a single
    /// `And(vec![a, b, c])` rather than nested binary nodes. An empty
    /// `Vec` evaluates to `true` (vacuous-truth identity).
    And(Vec<BasicPredicate<T>>),
    /// Logical disjunction. Flattened analogously to `And`. An empty
    /// `Vec` evaluates to `false` (no disjuncts succeed).
    Or(Vec<BasicPredicate<T>>),
    /// Logical negation. Double-negation collapses on construction.
    Not(Box<BasicPredicate<T>>),
    /// Exclusive or — `a XOR b`. Not flattened (XOR isn't associative
    /// the way And/Or are when chained).
    Xor(Box<BasicPredicate<T>>, Box<BasicPredicate<T>>),
}

impl<T> BasicPredicate<T> {
    /// Evaluate the predicate against an in-memory `&T`. Uses
    /// short-circuiting where possible (`And` stops at the first
    /// `false`, `Or` at the first `true`).
    ///
    /// Vacuous identities: empty `And(vec![])` returns `true`; empty
    /// `Or(vec![])` returns `false`. `True` returns `true`; `False`
    /// returns `false`; `Field` invokes the closure captured at
    /// construction time.
    pub fn evaluate(&self, value: &T) -> bool {
        match self {
            Self::True => true,
            Self::False => false,
            Self::Field(fp) => fp.evaluate(value),
            Self::And(children) => children.iter().all(|c| c.evaluate(value)),
            Self::Or(children) => children.iter().any(|c| c.evaluate(value)),
            Self::Not(inner) => !inner.evaluate(value),
            Self::Xor(a, b) => a.evaluate(value) ^ b.evaluate(value),
        }
    }
}

impl<T> BitAnd for BasicPredicate<T> {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::And(mut l), Self::And(r)) => {
                l.extend(r);
                Self::And(l)
            }
            (Self::And(mut l), r) => {
                l.push(r);
                Self::And(l)
            }
            (l, Self::And(mut r)) => {
                r.insert(0, l);
                Self::And(r)
            }
            (l, r) => Self::And(vec![l, r]),
        }
    }
}

impl<T> BitOr for BasicPredicate<T> {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Or(mut l), Self::Or(r)) => {
                l.extend(r);
                Self::Or(l)
            }
            (Self::Or(mut l), r) => {
                l.push(r);
                Self::Or(l)
            }
            (l, Self::Or(mut r)) => {
                r.insert(0, l);
                Self::Or(r)
            }
            (l, r) => Self::Or(vec![l, r]),
        }
    }
}

impl<T> BitXor for BasicPredicate<T> {
    type Output = Self;
    fn bitxor(self, rhs: Self) -> Self {
        Self::Xor(Box::new(self), Box::new(rhs))
    }
}

impl<T> Not for BasicPredicate<T> {
    type Output = Self;
    fn not(self) -> Self {
        match self {
            Self::Not(inner) => *inner, // double-negation collapse
            Self::True => Self::False,
            Self::False => Self::True,
            other => Self::Not(Box::new(other)),
        }
    }
}
