//! Versioned JSON wire envelope for L2 cache backends.
//!
//! Backends store `serde_json` bytes shaped as
//! `{ "__sassi_v": 0, "payload": T }`. The explicit major version lets
//! readers reject incompatible future formats before deserializing the
//! payload as the wrong type.

use crate::error::WireFormatError;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Current Sassi wire-format major version.
pub const WIRE_FORMAT_MAJOR: u64 = 0;

#[derive(Serialize)]
struct EnvelopeRef<'a, T: ?Sized> {
    #[serde(rename = "__sassi_v")]
    version: u64,
    payload: &'a T,
}

#[derive(Deserialize)]
struct Envelope<T> {
    #[serde(rename = "__sassi_v")]
    version: u64,
    payload: T,
}

/// Serialize a payload into Sassi's versioned JSON envelope.
///
/// # Errors
///
/// Returns [`WireFormatError::Serde`] when the payload cannot be
/// serialized as JSON.
pub fn to_vec<T: Serialize + ?Sized>(payload: &T) -> Result<Vec<u8>, WireFormatError> {
    let envelope = EnvelopeRef {
        version: WIRE_FORMAT_MAJOR,
        payload,
    };
    serde_json::to_vec(&envelope).map_err(WireFormatError::from)
}

/// Deserialize a payload from Sassi's versioned JSON envelope.
///
/// # Errors
///
/// Returns [`WireFormatError::VersionMismatch`] when the envelope was
/// written by an incompatible major format, or
/// [`WireFormatError::Serde`] when the bytes are not valid envelope
/// JSON for the requested payload type.
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, WireFormatError> {
    let envelope: Envelope<T> = serde_json::from_slice(bytes)?;
    if envelope.version != WIRE_FORMAT_MAJOR {
        return Err(WireFormatError::VersionMismatch {
            got: envelope.version,
            expected: WIRE_FORMAT_MAJOR,
        });
    }
    Ok(envelope.payload)
}
