//! [`TenantKey`] — adopter-owned tenant labels for applications that keep
//! tenant context beside Sassi handles.
//!
//! Sassi does not infer tenant isolation from cached values and does not
//! turn [`crate::punnu::PunnuConfig::namespace`] into an L1 tenancy boundary.
//! `PunnuConfig::namespace` is for L2 backend keyspace separation.
//!
//! Applications that need tenant or substrate separation should make that
//! boundary explicit: own separate pool handles per substrate, use distinct
//! wrapper types, choose a tenant-qualified id when identity itself is
//! tenant-qualified, or carry `TenantKey` alongside the pool/reference that
//! is allowed to serve a request.

/// Opaque tenant identifier.
///
/// A `TenantKey` is just a wrapper around `String`: no parsing, no schema,
/// and no built-in authorization semantics. Sassi never inspects the contents.
/// Consumers choose the encoding; typical patterns are tenant slugs (`"acme"`),
/// stable external ids, or environment-prefixed labels (`"prod_acme"`).
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
