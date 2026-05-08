#![cfg(feature = "serde")]

use sassi::{Cacheable, Field, InsertError, Punnu, WireFormatError, wire};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct E {
    id: i64,
    label: String,
}

#[derive(Default)]
struct EFields {
    #[allow(dead_code)]
    id: Field<E, i64>,
}

impl Cacheable for E {
    type Id = i64;
    type Fields = EFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> EFields {
        EFields {
            id: Field::new("id", |e| &e.id),
        }
    }
}

const MAGIC: &[u8; 8] = b"SASSI\0W\0";
const KIND_VALUE: u8 = 0x01;

/// Build a wire-shaped byte stream that satisfies the magic, kind, and
/// flags bytes for `T` but advertises a future major version followed by
/// junk in place of a postcard payload. Used to prove that header
/// validation rejects incompatible majors before the body decoder is
/// even reached.
fn future_major_with_bad_body<T: Cacheable>() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(wire::WIRE_FORMAT_MAJOR + 1).to_le_bytes());
    bytes.push(KIND_VALUE);
    bytes.push(0);
    let name = T::cache_type_name().as_bytes();
    bytes.extend_from_slice(&(name.len() as u16).to_le_bytes());
    bytes.extend_from_slice(name);
    bytes.extend_from_slice(b"not postcard payload");
    bytes
}

#[test]
fn wire_to_vec_should_emit_v1_header_before_payload() {
    let bytes = wire::to_vec(&E {
        id: 7,
        label: "seven".into(),
    })
    .unwrap();

    assert_eq!(
        &bytes[..8],
        MAGIC,
        "binary wire must start with Sassi magic"
    );
    assert_eq!(
        u16::from_le_bytes([bytes[8], bytes[9]]),
        wire::WIRE_FORMAT_MAJOR,
        "wire major must round-trip as little-endian u16"
    );
    assert_eq!(bytes[10], KIND_VALUE, "kind byte must be value-wire");
    assert_eq!(bytes[11], 0, "flags byte must be zero in v1");
    let name_len = u16::from_le_bytes([bytes[12], bytes[13]]) as usize;
    let header_end = 14 + name_len;
    let header_name = std::str::from_utf8(&bytes[14..header_end]).unwrap();
    assert_eq!(header_name, E::cache_type_name());
    assert!(
        bytes.len() > header_end,
        "header should be followed by a non-empty postcard body"
    );
}

#[test]
fn wire_from_slice_should_round_trip_v1_postcard_payload() {
    let original = E {
        id: 42,
        label: "answer".into(),
    };
    let bytes = wire::to_vec(&original).unwrap();

    let decoded: E = wire::from_slice(&bytes).unwrap();

    assert_eq!(decoded, original);
}

#[test]
fn wire_from_slice_should_reject_future_major_before_payload_decode() {
    let bytes = future_major_with_bad_body::<E>();

    let err = wire::from_slice::<E>(&bytes).unwrap_err();

    match err {
        WireFormatError::VersionMismatch { got, expected } => {
            assert_eq!(got, wire::WIRE_FORMAT_MAJOR + 1);
            assert_eq!(expected, wire::WIRE_FORMAT_MAJOR);
        }
        other => panic!("expected version mismatch, got {other:?}"),
    }
}

#[test]
fn wire_from_slice_should_reject_wrong_type_name_before_payload_decode() {
    // Build a header with a different type name but a valid postcard
    // body for `E`. The header type-name check must reject before the
    // postcard body would otherwise decode successfully.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&wire::WIRE_FORMAT_MAJOR.to_le_bytes());
    bytes.push(KIND_VALUE);
    bytes.push(0);
    let other = "myapp.NotE";
    bytes.extend_from_slice(&(other.len() as u16).to_le_bytes());
    bytes.extend_from_slice(other.as_bytes());
    let body = postcard::to_allocvec(&E {
        id: 5,
        label: "five".into(),
    })
    .unwrap();
    bytes.extend_from_slice(&body);

    let err = wire::from_slice::<E>(&bytes).unwrap_err();

    match err {
        WireFormatError::TypeNameMismatch { got, expected } => {
            assert_eq!(got, other);
            assert_eq!(expected, E::cache_type_name());
        }
        other => panic!("expected type-name mismatch, got {other:?}"),
    }
}

#[test]
fn wire_from_slice_should_reject_trailing_body_bytes() {
    let mut bytes = wire::to_vec(&E {
        id: 8,
        label: "eight".into(),
    })
    .unwrap();
    bytes.extend_from_slice(b"\xff\xff\xff");

    let err = wire::from_slice::<E>(&bytes).unwrap_err();

    match err {
        WireFormatError::Codec(message) => {
            assert!(
                message.contains("trailing bytes"),
                "expected trailing-byte rejection, got {message}"
            );
        }
        other => panic!("expected codec error for trailing bytes, got {other:?}"),
    }
}

#[test]
fn wire_from_slice_should_reject_legacy_json_v0_as_version_mismatch() {
    let beta_one_envelope = serde_json::json!({
        "__sassi_v": 0,
        "payload": { "id": 1, "label": "old" },
    });
    let bytes = serde_json::to_vec(&beta_one_envelope).unwrap();

    let err = wire::from_slice::<E>(&bytes).unwrap_err();

    match err {
        WireFormatError::VersionMismatch { got, expected } => {
            assert_eq!(got, 0, "beta.1 JSON envelopes report as wire major 0");
            assert_eq!(expected, wire::WIRE_FORMAT_MAJOR);
        }
        other => panic!("expected version mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn insert_serialized_should_deserialize_v1_payload_and_insert_value() {
    let pool = Punnu::<E>::builder().build();
    let bytes = wire::to_vec(&E {
        id: 9,
        label: "nine".into(),
    })
    .unwrap();

    let inserted = pool.insert_serialized(&bytes).await.unwrap();

    assert_eq!(inserted.id, 9);
    assert_eq!(inserted.label, "nine");
    assert_eq!(pool.get(&9).unwrap().label, "nine");
}

#[tokio::test]
async fn insert_serialized_should_reject_incompatible_major_without_inserting() {
    let pool = Punnu::<E>::builder().build();
    let bytes = future_major_with_bad_body::<E>();

    let err = pool.insert_serialized(&bytes).await.unwrap_err();

    match err {
        InsertError::WireFormat(WireFormatError::VersionMismatch { got, expected }) => {
            assert_eq!(got, wire::WIRE_FORMAT_MAJOR + 1);
            assert_eq!(expected, wire::WIRE_FORMAT_MAJOR);
        }
        other => panic!("expected wire-format version mismatch, got {other:?}"),
    }
    assert!(pool.get(&11).is_none());
}
