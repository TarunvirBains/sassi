//! Library-wide error types.
//!
//! Sassi's public errors live here so they can compose freely across
//! modules without cyclic-module headaches. The full set lands across
//! several tasks (Punnu, single-flight, backend, wire format); this
//! module grows as those tasks ship.
//!
//! Variants present today:
//! - [`InsertError`] — surfaced from [`crate::punnu::Punnu::insert`] and friends.
//! - [`BackendError`] — surfaced from the [`CacheBackend`](crate) trait
//!   (full trait lands in a later task; the variants are pinned now so
//!   error types that compose with it are stable from v0.1.0-alpha.0
//!   onward).
//!
//! Sassi's error doctrine matches the Rust ecosystem standard:
//! `thiserror`-derived enums for library types, with `#[error("…")]`
//! messages that tell the caller what they need to know without
//! leaking implementation detail.

use thiserror::Error;

/// Reasons a [`crate::punnu::Punnu::insert`] (or the L2 write-through
/// behind it) can fail.
///
/// The default L1-only configuration only ever produces
/// [`InsertError::Conflict`] (when the pool is configured with
/// [`crate::punnu::OnConflict::Reject`] and an entry with the same id is
/// already present). [`InsertError::Serialization`] and
/// [`InsertError::BackendFailed`] become reachable when an L2 backend
/// is wired up — the variants live here from day one so the error type
/// is stable before backends land.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InsertError {
    /// Conflict — the pool was configured with
    /// [`crate::punnu::OnConflict::Reject`] and an entry with the same
    /// id is already cached. Default conflict policy is
    /// `LastWriteWins`; this variant is unreachable unless the consumer
    /// opts in to `Reject`.
    #[error("entry conflict — OnConflict::Reject configured and id is already cached")]
    Conflict,

    /// Serialization failed when writing through to the L2 backend.
    /// Carries a human-readable reason; the backend chooses the wording.
    #[error("serialization failed during insert: {0}")]
    Serialization(String),

    /// L2 backend write-through failed and
    /// [`crate::punnu::BackendFailureMode::Error`] is configured. With
    /// the default [`crate::punnu::BackendFailureMode::L1Only`], backend
    /// errors are logged and swallowed — `insert` succeeds against L1
    /// alone.
    #[error("backend write-through failed: {0}")]
    BackendFailed(#[from] BackendError),
}

/// Errors from the [`CacheBackend`](crate) trait surface.
///
/// The full backend trait lands in a later task; the variants are
/// pinned here so types that compose with it (notably
/// [`InsertError::BackendFailed`]) have a stable shape from
/// v0.1.0-alpha.0 onward. Backends choose the variant that best matches
/// the underlying failure; consumers pattern-match on the variant
/// rather than parsing the message.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BackendError {
    /// The backend reports the entry is absent — distinct from a
    /// transport failure. Surfaces as a `None` to the caller in most
    /// flows; the dedicated variant exists so backends with strict
    /// existence semantics can be unambiguous.
    #[error("backend reports entry not found")]
    NotFound,

    /// The backend could not serialize or deserialize a payload. The
    /// inner string is backend-supplied (e.g., serde error rendering).
    #[error("backend serialization error: {0}")]
    Serialization(String),

    /// Network / IO transport error. Inner string is backend-supplied.
    #[error("backend network error: {0}")]
    Network(String),

    /// Anything that doesn't fit the variants above. Boxed so the
    /// variant size stays small.
    #[error("backend error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}
