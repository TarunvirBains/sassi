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
//!   and from [`crate::punnu::Punnu::get_async`].
//!
//! Sassi's error doctrine matches the Rust ecosystem standard:
//! `thiserror`-derived enums for library types, with `#[error("…")]`
//! messages that tell the caller what they need to know without
//! leaking implementation detail.

use thiserror::Error;

/// Errors produced by Sassi's binary wire container.
///
/// Sassi values cross runtime and process boundaries inside a fixed
/// binary header followed by a postcard-encoded body. The header
/// captures the wire major, kind discriminator, optional flags, and the
/// cached type name so readers can reject incompatible payloads before
/// touching the body. Postcard's own error type is intentionally not
/// part of the public surface — its detail is folded into [`Codec`] so
/// the wire format can evolve without leaking the codec choice.
///
/// [`Codec`]: WireFormatError::Codec
#[cfg(feature = "serde")]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WireFormatError {
    /// The wire major is not understood by this crate.
    #[error("wire format major version mismatch: got {got}, expected {expected}")]
    VersionMismatch {
        /// Major version found in the wire header.
        got: u16,
        /// Major version this crate can read.
        expected: u16,
    },

    /// The byte stream does not start with Sassi's wire magic.
    #[error("wire format has invalid magic header")]
    InvalidMagic,

    /// The byte stream is a different Sassi wire kind than the caller expected.
    #[error("wire format kind mismatch: got {got}, expected {expected}")]
    KindMismatch {
        /// Kind byte found in the wire header.
        got: u8,
        /// Kind byte required by the decode path.
        expected: u8,
    },

    /// The byte stream uses a reserved or unsupported wire kind.
    #[error("wire format kind is reserved or unsupported: {kind}")]
    UnsupportedKind {
        /// Reserved or unsupported kind byte.
        kind: u8,
    },

    /// The byte stream sets flags this crate does not understand.
    #[error("wire format flags are unsupported: {flags}")]
    UnsupportedFlags {
        /// Unsupported flags byte.
        flags: u8,
    },

    /// The header names a different cached type.
    #[error("wire format type mismatch: got {got}, expected {expected}")]
    TypeNameMismatch {
        /// Type name found in the wire header.
        got: String,
        /// Type name required by the decode path.
        expected: &'static str,
    },

    /// The fixed header or variable type-name segment is malformed.
    #[error("wire header is malformed: {0}")]
    MalformedHeader(String),

    /// The postcard body failed to encode or decode.
    #[error("wire body codec error: {0}")]
    Codec(String),
}

/// Reasons a [`crate::punnu::Punnu::insert`] (or the L2 write-through
/// behind it) can fail.
///
/// The default L1-only configuration only ever produces
/// [`InsertError::Conflict`] (when the pool is configured with
/// [`crate::punnu::OnConflict::Reject`] and an entry with the same id is
/// already present). [`InsertError::WireFormat`] is also reachable from
/// [`crate::punnu::Punnu::insert_serialized`] before any L2 backend is
/// involved. [`InsertError::Serialization`] and
/// [`InsertError::BackendFailed`] become reachable when an L2 backend is
/// attached.
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

    /// Sassi's binary wire container failed to serialize or
    /// deserialize the payload.
    #[cfg(feature = "serde")]
    #[error("wire-format error: {0}")]
    WireFormat(#[from] WireFormatError),
}

/// Errors produced while restoring a Punnu entries snapshot.
///
/// `Punnu::restore_entries_postcard(bytes)` is L1-only and synchronous;
/// it rejects snapshots that cannot be applied as a whole-pool replace
/// before mutating any state. The variants here cover the rejection
/// modes — wire-format trouble, snapshot-shape problems, and a strict
/// backend write race that prevents a synchronous restore. Here "strict"
/// means an active backend write reservation from a pool configured with
/// [`crate::punnu::BackendFailureMode::Error`].
#[cfg(feature = "serde")]
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PunnuSnapshotError {
    /// The snapshot wire container could not be decoded for this cached type.
    #[error("punnu snapshot wire-format error: {0}")]
    WireFormat(#[from] WireFormatError),

    /// The snapshot contains the same cache id more than once.
    #[error("punnu snapshot contains duplicate id")]
    DuplicateId,

    /// The snapshot has more entries than the receiving pool can hold.
    #[error("punnu snapshot contains {entries} entries but this pool allows at most {limit}")]
    TooManyEntries {
        /// Entry count found in the snapshot.
        entries: usize,
        /// Receiving pool capacity.
        limit: usize,
    },

    /// The receiving pool has an in-flight strict backend write and cannot
    /// restore synchronously.
    #[error(
        "punnu snapshot restore cannot run while {reserved} strict backend write(s) are in flight"
    )]
    BackendWriteInFlight {
        /// Number of strict backend write reservations currently active.
        reserved: usize,
    },
}

/// Reasons a [`crate::punnu::Punnu::get_or_fetch`] (or batch variant)
/// can fail.
///
/// Carries either a backend error from the L2 path, a serialization
/// failure (e.g., when the binary wire container rejects a payload), a
/// fetcher panic surfaced via the single-flight follower path, or an
/// arbitrary boxed error supplied by the consumer's fetcher closure.
///
/// Single-flight owner-loss is deterministic: `FetcherPanic` is the case that
/// surfaces here. Originator-drop-with-peers, all-awaiters-drop, and
/// caller-imposed deadlines do not produce a `FetchError`; they either leave
/// the fetch alive, drop it cleanly, or surface as a timeout error from the
/// caller's own wrapper.
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

    /// The consumer-supplied fetcher closure panicked. Sassi catches the
    /// unwind inside the single-flight owner future and translates it
    /// into this structured error for every attached awaiter, so one
    /// panicking fetcher cannot strand peers on a dropped shared future.
    /// The `type_name` is `std::any::type_name::<T>()` of the cached
    /// type — useful as a diagnostic label.
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

    /// L1 insert failed after a fetcher returned a value. Today this is
    /// reachable from the batch fetch path when
    /// [`crate::punnu::OnConflict::Reject`] is configured and a
    /// concurrent insert raced ahead. The single-id
    /// [`crate::punnu::Punnu::get_or_fetch`] path uses
    /// `insert_arc_or_existing` and returns the already-cached value
    /// instead of surfacing a conflict. L2 write-through can also lift
    /// [`InsertError`] into the fetch error space.
    #[error("L1 insert failed during fetch: {0}")]
    Insert(#[from] InsertError),
}

/// Errors from the [`CacheBackend`](crate) trait surface.
///
/// Backends choose the variant that best matches the underlying
/// failure; consumers pattern-match on the variant rather than parsing
/// the message.
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

    /// The backend encountered a Sassi binary wire-container error.
    #[cfg(feature = "serde")]
    #[error("backend wire-format error: {0}")]
    WireFormat(#[from] WireFormatError),

    /// Network / IO transport error. Inner string is backend-supplied.
    #[error("backend network error: {0}")]
    Network(String),

    /// Anything that doesn't fit the variants above. Boxed so the
    /// variant size stays small.
    #[error("backend error: {0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(feature = "serde")]
impl From<serde_json::Error> for BackendError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}
