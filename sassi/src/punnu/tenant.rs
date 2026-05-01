//! [`TenantKey`] — multi-tenant identity for [`crate::punnu::Punnu`]
//! instances.
//!
//! v0.1 of this cluster ships the type alone; cross-tenant access
//! guards (Doctrine C in spec §3.1.1) wire up alongside the scope /
//! orchestrator surface in a later task.
//!
//! Sassi's tenant model is "tenant identity is part of the cache key" —
//! materialised by **namespacing the `Punnu` instance**, not by adding
//! a tenant column inside the cached value. Doctrine C: a
//! single-tenant `Punnu` and a per-tenant `Punnu` are different types
//! at construction. The pair `(TenantKey, Punnu<T>)` is the canonical
//! shape; consumers carry the tenant key alongside the pool reference
//! and never thread it through `T`.

/// Opaque tenant identifier.
///
/// A `TenantKey` is just a wrapper around `String` — no parsing, no
/// schema. Sassi never inspects the contents; it's a label that flows
/// through the cache-key namespace and (in a later task) the
/// cross-tenant access guards. Consumers choose the encoding —
/// typical patterns are tenant slugs (`"acme"`), HeerId-rendered ids
/// (`"H7K..."`), or environment-prefixed combos (`"prod_acme"`).
///
/// # Construction
///
/// Use the standard `From<String>` / `From<&str>` impls, or
/// [`TenantKey::none`] for the "single-tenant pool" sentinel.
///
/// ```
/// use sassi::TenantKey;
///
/// let acme: TenantKey = "acme".into();
/// assert_eq!(acme.as_str(), "acme");
/// assert!(TenantKey::none().is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantKey(String);

impl TenantKey {
    /// Construct from anything that converts into a `String`.
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Sentinel for "no tenant" — equivalent to a single-tenant pool.
    /// Returns `None` so the caller can branch on tenancy without
    /// constructing a placeholder string.
    pub fn none() -> Option<Self> {
        None
    }

    /// Borrow the underlying string. Useful for cache-key composition
    /// and for diagnostics.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Move the underlying string out of the wrapper.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for TenantKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TenantKey {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl std::fmt::Display for TenantKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
