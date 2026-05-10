//! Binary wire container for L2 cache backends and other byte-shaped
//! transfers.
//!
//! Sassi values cross runtime, process, and storage boundaries inside a
//! fixed binary header followed by a postcard-encoded body. The header
//! carries a magic prefix, a little-endian wire major, a kind byte, a
//! flags byte, and the cached type name from
//! [`Cacheable::cache_type_name`]. Readers validate the header before
//! decoding the body so an incompatible payload can never be misread as
//! the requested type.
//!
//! Wire majors are independent of the crate's semver. The current major
//! is exposed as [`WIRE_FORMAT_MAJOR`].
//!
//! [`Cacheable::cache_type_name`]: crate::Cacheable::cache_type_name

use crate::cacheable::Cacheable;
use crate::error::WireFormatError;
use serde::{Serialize, de::DeserializeOwned};

/// Current Sassi binary wire-format major version.
pub const WIRE_FORMAT_MAJOR: u16 = 1;

const MAGIC: &[u8; 8] = b"SASSI\0W\0";
pub(crate) const KIND_VALUE: u8 = 0x01;
pub(crate) const KIND_FILE_ENTRY: u8 = 0x02;
pub(crate) const KIND_PUNNU_ENTRIES: u8 = 0x03;
pub(crate) const KIND_PUNNU_ENTRIES_WITH_HINTS: u8 = 0x04;
/// First kind byte that the current implementation does not understand.
/// Anything `>=` this value is rejected as an unsupported wire kind so
/// future kinds cannot be silently misread.
const FIRST_RESERVED_KIND: u8 = 0x05;
const HEADER_FIXED_LEN: usize = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireKind {
    Value,
    FileEntry,
    PunnuEntries,
    PunnuEntriesWithHints,
}

impl WireKind {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            Self::Value => KIND_VALUE,
            Self::FileEntry => KIND_FILE_ENTRY,
            Self::PunnuEntries => KIND_PUNNU_ENTRIES,
            Self::PunnuEntriesWithHints => KIND_PUNNU_ENTRIES_WITH_HINTS,
        }
    }
}

pub(crate) fn encode_header<T: Cacheable>(
    kind: WireKind,
    out: &mut Vec<u8>,
) -> Result<(), WireFormatError> {
    let type_name = T::cache_type_name().as_bytes();
    let len: u16 = type_name.len().try_into().map_err(|_| {
        WireFormatError::MalformedHeader("cache type name exceeds u16 length".into())
    })?;

    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&WIRE_FORMAT_MAJOR.to_le_bytes());
    out.push(kind.as_u8());
    out.push(0);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(type_name);
    Ok(())
}

pub(crate) fn decode_header<T: Cacheable>(
    bytes: &[u8],
    expected: WireKind,
) -> Result<&[u8], WireFormatError> {
    // Beta.1 wrote JSON envelopes that always started with `{`. Surface
    // those as a wire-major mismatch rather than `InvalidMagic` so the
    // upgrade story stays focused on "the major changed."
    if bytes.first() == Some(&b'{') {
        return Err(WireFormatError::VersionMismatch {
            got: 0,
            expected: WIRE_FORMAT_MAJOR,
        });
    }
    if bytes.len() < HEADER_FIXED_LEN {
        return Err(WireFormatError::MalformedHeader("header too short".into()));
    }
    if &bytes[..8] != MAGIC {
        return Err(WireFormatError::InvalidMagic);
    }

    let major = u16::from_le_bytes([bytes[8], bytes[9]]);
    if major != WIRE_FORMAT_MAJOR {
        return Err(WireFormatError::VersionMismatch {
            got: major,
            expected: WIRE_FORMAT_MAJOR,
        });
    }

    let kind = bytes[10];
    // The current implementation understands `Value`, `FileEntry`,
    // `PunnuEntries`, and `PunnuEntriesWithHints`. Anything at or above
    // [`FIRST_RESERVED_KIND`] is future-only and must be rejected so a
    // forward kind cannot be silently misread as one of the supported
    // shapes. Reading still pre-checks the kind byte before any
    // type-name or body decode work.
    if kind >= FIRST_RESERVED_KIND {
        return Err(WireFormatError::UnsupportedKind { kind });
    }
    if kind != expected.as_u8() {
        return Err(WireFormatError::KindMismatch {
            got: kind,
            expected: expected.as_u8(),
        });
    }

    let flags = bytes[11];
    if flags != 0 {
        return Err(WireFormatError::UnsupportedFlags { flags });
    }

    let name_len = u16::from_le_bytes([bytes[12], bytes[13]]) as usize;
    let name_start = HEADER_FIXED_LEN;
    let name_end = name_start + name_len;
    if bytes.len() < name_end {
        return Err(WireFormatError::MalformedHeader(
            "type name extends past input".into(),
        ));
    }
    let got = std::str::from_utf8(&bytes[name_start..name_end])
        .map_err(|err| WireFormatError::MalformedHeader(err.to_string()))?;
    let expected_name = T::cache_type_name();
    if got != expected_name {
        return Err(WireFormatError::TypeNameMismatch {
            got: got.to_owned(),
            expected: expected_name,
        });
    }

    Ok(&bytes[name_end..])
}

pub(crate) fn decode_postcard_exact<T>(body: &[u8]) -> Result<T, WireFormatError>
where
    T: DeserializeOwned,
{
    let (value, trailing) =
        postcard::take_from_bytes(body).map_err(|err| WireFormatError::Codec(err.to_string()))?;
    if !trailing.is_empty() {
        return Err(WireFormatError::Codec(
            "trailing bytes after postcard body".into(),
        ));
    }
    Ok(value)
}

/// Serialize a cacheable payload into Sassi's binary value wire container.
///
/// The output starts with a fixed binary header (magic, wire major,
/// kind byte, flags, and `T::cache_type_name()`) followed by a
/// postcard-encoded body. Readers can validate the header before
/// decoding the body, so payloads that name a different cached type
/// or use an incompatible wire major are rejected without touching the
/// body bytes.
///
/// # Errors
///
/// Returns [`WireFormatError::MalformedHeader`] if `T::cache_type_name()`
/// exceeds the header's `u16` length budget, or
/// [`WireFormatError::Codec`] if postcard fails to encode the payload.
pub fn to_vec<T>(payload: &T) -> Result<Vec<u8>, WireFormatError>
where
    T: Cacheable + Serialize,
{
    let mut out = Vec::new();
    encode_header::<T>(WireKind::Value, &mut out)?;
    append_postcard(payload, &mut out)?;
    Ok(out)
}

pub(crate) fn append_postcard<T>(payload: &T, out: &mut Vec<u8>) -> Result<(), WireFormatError>
where
    T: Serialize + ?Sized,
{
    let body =
        postcard::to_allocvec(payload).map_err(|err| WireFormatError::Codec(err.to_string()))?;
    out.extend_from_slice(&body);
    Ok(())
}

/// Deserialize a cacheable payload from Sassi's binary value wire container.
///
/// Validates the header before decoding the body. The header guards
/// against version drift, kind confusion (e.g., decoding a file-entry
/// body as a value), corrupt flag bits, and type-name mismatch. After
/// the header passes, the body is decoded with postcard and any trailing
/// bytes after the payload are rejected as a codec error.
///
/// # Errors
///
/// - [`WireFormatError::VersionMismatch`] when the wire major differs
///   from [`WIRE_FORMAT_MAJOR`] (including beta.1 JSON bytes, which
///   start with `{` and are reported as version `0`).
/// - [`WireFormatError::InvalidMagic`] when the leading magic bytes do
///   not match Sassi's prefix.
/// - [`WireFormatError::KindMismatch`] /
///   [`WireFormatError::UnsupportedKind`] when the kind byte is not the
///   expected value-wire kind.
/// - [`WireFormatError::UnsupportedFlags`] when the header flags are
///   non-zero.
/// - [`WireFormatError::TypeNameMismatch`] when the header names a
///   different cached type than `T::cache_type_name()`.
/// - [`WireFormatError::MalformedHeader`] when the header or
///   variable type-name segment is truncated or non-UTF-8.
/// - [`WireFormatError::Codec`] when postcard fails to decode the body
///   or trailing bytes are present after the payload.
pub fn from_slice<T>(bytes: &[u8]) -> Result<T, WireFormatError>
where
    T: Cacheable + DeserializeOwned,
{
    let body = decode_header::<T>(bytes, WireKind::Value)?;
    decode_postcard_exact(body)
}

/// Encode a Punnu entries snapshot body.
///
/// The body shape is `<little-endian u32 count> <count × postcard(T)>`
/// after the shared binary header. Borrowed `&T` values keep the
/// caller-owned `Arc<T>` snapshot alive during serialization without
/// requiring `T: Clone`.
pub(crate) fn encode_punnu_entries<T>(entries: &[&T]) -> Result<Vec<u8>, WireFormatError>
where
    T: Cacheable + Serialize,
{
    let mut out = Vec::new();
    encode_header::<T>(WireKind::PunnuEntries, &mut out)?;
    let count = u32::try_from(entries.len())
        .map_err(|_| WireFormatError::Codec("too many punnu entries".into()))?;
    out.extend_from_slice(&count.to_le_bytes());
    for entry in entries {
        append_postcard(*entry, &mut out)?;
    }
    Ok(out)
}

/// Decode the entries-snapshot header and entry-count prefix without
/// touching the per-entry postcard bytes.
///
/// Returns the decoded entry count and the slice positioned at the
/// first per-entry postcard payload. Splitting the count from the
/// per-entry decode lets callers reject oversized snapshots before
/// allocating or deserializing every entry.
pub(crate) fn decode_punnu_entries_len<T>(bytes: &[u8]) -> Result<(usize, &[u8]), WireFormatError>
where
    T: Cacheable,
{
    let body = decode_header::<T>(bytes, WireKind::PunnuEntries)?;
    if body.len() < 4 {
        return Err(WireFormatError::MalformedHeader(
            "punnu entries body missing count".into(),
        ));
    }
    let count = u32::from_le_bytes(body[..4].try_into().expect("slice length checked")) as usize;
    Ok((count, &body[4..]))
}

/// Decode `count` postcard-encoded entries from a snapshot body slice.
///
/// `count` is decoded from the wire format and is treated as untrusted
/// even after the caller's `count <= lru_size` rejection: a consumer
/// may legitimately configure a very large `lru_size`, and a malformed
/// or hostile snapshot can declare a count near that bound while
/// providing little or no body. To prevent a process-level abort or
/// capacity-overflow panic on the speculative allocation, this function
/// uses [`Vec::try_reserve_exact`] so allocator failure becomes a
/// recoverable [`WireFormatError::Codec`] rather than a panic.
///
/// Trailing bytes after the final entry are rejected as a codec error
/// so a body that promises N entries but contains stray bytes cannot
/// be silently accepted.
pub(crate) fn decode_punnu_entries_body<T>(
    mut body: &[u8],
    count: usize,
) -> Result<Vec<T>, WireFormatError>
where
    T: Cacheable + DeserializeOwned,
{
    let mut entries: Vec<T> = Vec::new();
    entries.try_reserve_exact(count).map_err(|err| {
        WireFormatError::Codec(format!(
            "could not reserve capacity for {count} punnu entries: {err}"
        ))
    })?;
    for _ in 0..count {
        let (entry, rest) = postcard::take_from_bytes(body)
            .map_err(|err| WireFormatError::Codec(err.to_string()))?;
        entries.push(entry);
        body = rest;
    }
    if !body.is_empty() {
        return Err(WireFormatError::Codec(
            "trailing bytes after punnu entries body".into(),
        ));
    }
    Ok(entries)
}

/// Peek at the kind byte of a Sassi wire container without validating
/// the type name or decoding the body.
///
/// The header is still validated for magic and wire-major before the
/// kind byte is returned, so callers cannot accidentally treat a
/// non-Sassi byte stream or a future major as a known kind. The
/// returned byte is the raw kind discriminator; map it to a
/// [`WireKind`] internally if dispatching.
///
/// Used by the snapshot/restore wrapper to dispatch between
/// entries-only and internal-state restore paths from one byte stream.
pub(crate) fn peek_kind(bytes: &[u8]) -> Result<u8, WireFormatError> {
    if bytes.first() == Some(&b'{') {
        return Err(WireFormatError::VersionMismatch {
            got: 0,
            expected: WIRE_FORMAT_MAJOR,
        });
    }
    if bytes.len() < HEADER_FIXED_LEN {
        return Err(WireFormatError::MalformedHeader("header too short".into()));
    }
    if &bytes[..8] != MAGIC {
        return Err(WireFormatError::InvalidMagic);
    }
    let major = u16::from_le_bytes([bytes[8], bytes[9]]);
    if major != WIRE_FORMAT_MAJOR {
        return Err(WireFormatError::VersionMismatch {
            got: major,
            expected: WIRE_FORMAT_MAJOR,
        });
    }
    let kind = bytes[10];
    if kind >= FIRST_RESERVED_KIND {
        return Err(WireFormatError::UnsupportedKind { kind });
    }
    Ok(kind)
}

/// Decode the with-hints snapshot header and return the body slice.
///
/// Validates the shared header fields (magic, wire major, kind, flags,
/// type name) for the [`WireKind::PunnuEntriesWithHints`] kind byte.
/// The body shape (envelope version, entry count, hint payload) is the
/// internal-state contract owned by [`crate::punnu`] and decoded there.
pub(crate) fn decode_punnu_entries_with_hints<T>(bytes: &[u8]) -> Result<&[u8], WireFormatError>
where
    T: Cacheable,
{
    decode_header::<T>(bytes, WireKind::PunnuEntriesWithHints)
}

/// Encode the shared binary header for a with-hints snapshot.
///
/// Appends only the header segment to `out`. Callers append their own
/// internal-state body (entry count, per-entry hint payload, etc.) after
/// this prefix.
pub(crate) fn encode_punnu_entries_with_hints_header<T>(
    out: &mut Vec<u8>,
) -> Result<(), WireFormatError>
where
    T: Cacheable,
{
    encode_header::<T>(WireKind::PunnuEntriesWithHints, out)
}
