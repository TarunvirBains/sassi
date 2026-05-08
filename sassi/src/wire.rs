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
const HEADER_FIXED_LEN: usize = 14;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WireKind {
    Value,
    FileEntry,
    // `PunnuEntries` is reserved for the entries snapshot kind shipped
    // by the Punnu export/restore APIs. It is constructed by the
    // entries-export code path; the `dead_code` allow keeps the
    // value-wire-only build (and the staging build before
    // `Punnu::export_entries_postcard` lands) clean.
    #[allow(dead_code)]
    PunnuEntries,
}

impl WireKind {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            Self::Value => KIND_VALUE,
            Self::FileEntry => KIND_FILE_ENTRY,
            Self::PunnuEntries => KIND_PUNNU_ENTRIES,
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
    // Beta.2 understands `Value`, `FileEntry`, and `PunnuEntries` only.
    // `KIND_PUNNU_ENTRIES_WITH_HINTS` is reserved for a future
    // operational-hints kind and unsupported; anything above it is
    // future-only, so collapse both branches into a single `>=` reject.
    if kind >= KIND_PUNNU_ENTRIES_WITH_HINTS {
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
