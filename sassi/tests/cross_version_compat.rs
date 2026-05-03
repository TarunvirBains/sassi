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

#[test]
fn wire_to_vec_should_store_payload_inside_v0_envelope() {
    let bytes = wire::to_vec(&E {
        id: 7,
        label: "seven".into(),
    })
    .unwrap();

    let envelope: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(envelope["__sassi_v"], 0);
    assert_eq!(envelope["payload"]["id"], 7);
    assert_eq!(envelope["payload"]["label"], "seven");
}

#[test]
fn wire_from_slice_should_round_trip_v0_envelope_payload() {
    let original = E {
        id: 42,
        label: "answer".into(),
    };
    let bytes = wire::to_vec(&original).unwrap();

    let decoded: E = wire::from_slice(&bytes).unwrap();

    assert_eq!(decoded, original);
}

#[test]
fn wire_from_slice_should_reject_incompatible_major_version() {
    let incompatible = serde_json::json!({
        "__sassi_v": 99,
        "payload": { "id": 1, "label": "old-new" }
    });
    let bytes = serde_json::to_vec(&incompatible).unwrap();

    let err = wire::from_slice::<E>(&bytes).unwrap_err();

    match err {
        WireFormatError::VersionMismatch { got, expected } => {
            assert_eq!(got, 99);
            assert_eq!(expected, wire::WIRE_FORMAT_MAJOR);
        }
        other => panic!("expected version mismatch, got {other:?}"),
    }
}

#[test]
fn wire_from_slice_should_check_major_before_payload_shape() {
    let incompatible = serde_json::json!({
        "__sassi_v": wire::WIRE_FORMAT_MAJOR + 1,
        "future_payload": {
            "identity": "not-the-v0-shape"
        }
    });
    let bytes = serde_json::to_vec(&incompatible).unwrap();

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
fn wire_error_should_convert_to_insert_serialization_error() {
    let err = WireFormatError::VersionMismatch {
        got: 1,
        expected: 0,
    };

    let insert: InsertError = err.into();

    match insert {
        InsertError::WireFormat(WireFormatError::VersionMismatch { got, expected }) => {
            assert_eq!(got, 1);
            assert_eq!(expected, 0);
        }
        other => panic!("expected insert serialization error, got {other:?}"),
    }
}

#[tokio::test]
async fn insert_serialized_should_deserialize_v0_envelope_and_insert_value() {
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
    let incompatible = serde_json::json!({
        "__sassi_v": wire::WIRE_FORMAT_MAJOR + 1,
        "payload": { "id": 11, "label": "future" }
    });
    let bytes = serde_json::to_vec(&incompatible).unwrap();

    let err = pool.insert_serialized(&bytes).await.unwrap_err();

    match err {
        InsertError::WireFormat(WireFormatError::VersionMismatch { got, expected }) => {
            assert_eq!(got, wire::WIRE_FORMAT_MAJOR + 1);
            assert_eq!(expected, wire::WIRE_FORMAT_MAJOR);
        }
        other => panic!("expected version mismatch, got {other:?}"),
    }
    assert!(pool.get(&11).is_none());
}

#[tokio::test]
async fn insert_serialized_should_check_major_before_payload_shape() {
    let pool = Punnu::<E>::builder().build();
    let incompatible = serde_json::json!({
        "__sassi_v": wire::WIRE_FORMAT_MAJOR + 1,
        "future_payload": {
            "identity": "not-the-v0-shape"
        }
    });
    let bytes = serde_json::to_vec(&incompatible).unwrap();

    let err = pool.insert_serialized(&bytes).await.unwrap_err();

    match err {
        InsertError::WireFormat(WireFormatError::VersionMismatch { got, expected }) => {
            assert_eq!(got, wire::WIRE_FORMAT_MAJOR + 1);
            assert_eq!(expected, wire::WIRE_FORMAT_MAJOR);
        }
        other => panic!("expected version mismatch, got {other:?}"),
    }
}
