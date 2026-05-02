//! Library-wide error types.
//!
//! Sassi's public errors live here so they can compose freely across
//! modules without cyclic-module headaches. The full set lands across
//! several tasks (Punnu, single-flight, backend, wire format); this
//! module grows as those tasks ship.
//!
//! Variants present today:
//! - [`InsertError`] — surfaced from [`crate::punnu::Punnu::insert`] and friends.
//! - [`FetchError`] — surfaced from [`crate::punnu::Punnu::get_or_fetch`]
//!   and batch fetch helpers.
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

/// Reasons a [`crate::punnu::Punnu::get_or_fetch`] (or batch variant)
/// can fail.
///
/// Carries either a backend error from the L2 path, a serialization
/// failure (e.g., when the wire-format envelope rejects a payload), a
/// fetcher panic surfaced via the single-flight follower path, or an
/// arbitrary boxed error supplied by the consumer's fetcher closure.
///
/// Spec §3.5.1 enumerates the four owner-loss cases the single-flight
/// path must handle deterministically; `FetcherPanic` is the one that
/// surfaces here. The other three (originator-drop-with-peers,
/// all-awaiters-drop, caller-imposed-deadline) don't produce a
/// `FetchError` — they either leave the fetch alive, drop it cleanly,
/// or surface as a `tokio::time::error::Elapsed` from the caller's
/// own `tokio::time::timeout` wrapper.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FetchError {
    /// L2 backend operation failed during the fetch path.
    #[error("backend operation failed during fetch: {0}")]
    Backend(#[from] BackendError),

    /// Serialization / deserialization of a fetched payload failed.
    /// Inner string is consumer-supplied (e.g., serde error rendering).
    #[error("fetch serialization error: {0}")]
    Serialization(String),

    /// The consumer-supplied fetcher closure panicked. Sassi does not
    /// `catch_unwind` around the fetcher (that would mask programmer
    /// bugs); this variant surfaces only via the
    /// [`futures::future::Shared`] follower-broadcast path described in
    /// spec §3.5.1: when the fetcher panics, *every* awaiter sees this
    /// error rather than one peer hanging on a dropped future. The
    /// `type_name` is `std::any::type_name::<T>()` of the cached type
    /// — useful as a diagnostic label.
    #[error("fetcher panicked while resolving {type_name}: {message}")]
    FetcherPanic {
        /// `std::any::type_name::<T>()` of the cached type.
        type_name: &'static str,
        /// Best-effort panic-payload message (extracted from the
        /// panic's `Box<dyn Any>`); empty when the payload isn't a
        /// `String` / `&'static str`.
        message: String,
    },

    /// The fetcher returned a value whose [`crate::Cacheable::id`]
    /// did not match the requested canonical id. Carries only the
    /// cached type name so this error remains available for all
    /// `Cacheable` ids; it does not require `T::Id: Debug`.
    #[error("fetcher returned a value whose id did not match the requested id for {type_name}")]
    IdentityMismatch {
        /// `std::any::type_name::<T>()` of the cached type.
        type_name: &'static str,
    },

    /// The consumer's fetcher returned a custom error. Boxed so the
    /// variant size stays small. Use [`FetchError::Custom`] when none
    /// of the structured variants fit (transport errors specific to
    /// the consumer's data source, business-logic rejections, etc.).
    #[error("fetcher error: {0}")]
    Custom(Box<dyn std::error::Error + Send + Sync>),

    /// L1 insert failed after the fetcher returned a value — surfaces
    /// when [`crate::punnu::OnConflict::Reject`] is configured and a
    /// concurrent insert raced ahead, or when an L2 write-through
    /// fails under [`crate::punnu::BackendFailureMode::Error`]. Lifts
    /// [`InsertError`] into the fetch error space so
    /// `Punnu::get_or_fetch` can return a single error type.
    #[error("L1 insert failed during fetch: {0}")]
    Insert(#[from] InsertError),
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
