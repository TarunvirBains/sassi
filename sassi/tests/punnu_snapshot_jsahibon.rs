#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use sassi::{Cacheable, Field, JObject, JSahibON, Punnu, PunnuRestoreStats, SnapshotMode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct JsonDoc {
    id: i64,
    payload: JSahibON,
    maybe_payload: Option<JSahibON>,
}

#[derive(Default)]
struct JsonDocFields {
    #[allow(dead_code)]
    id: Field<JsonDoc, i64>,
}

impl Cacheable for JsonDoc {
    type Id = i64;
    type Fields = JsonDocFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> JsonDocFields {
        JsonDocFields {
            id: Field::new("id", |doc| &doc.id),
        }
    }
}

fn object(entries: impl IntoIterator<Item = (&'static str, JSahibON)>) -> JSahibON {
    JSahibON::Object(JObject::from_entries(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value)),
    ))
}

fn docs() -> Vec<JsonDoc> {
    vec![
        JsonDoc {
            id: 1,
            payload: object([
                ("null", JSahibON::Null),
                ("bool", JSahibON::Bool(true)),
                ("i64", JSahibON::I64(-7)),
                ("u64", JSahibON::U64(u64::MAX)),
                ("f64", JSahibON::try_f64(1.25).unwrap()),
                ("string", JSahibON::String("hello".to_owned())),
                (
                    "array",
                    JSahibON::Array(vec![JSahibON::Null, JSahibON::String("nested".to_owned())]),
                ),
                ("object", object([("inner", JSahibON::Bool(false))])),
                ("cafe\u{301}", JSahibON::String("unicode-key".to_owned())),
            ]),
            maybe_payload: Some(JSahibON::Null),
        },
        JsonDoc {
            id: 2,
            payload: JSahibON::Array(vec![
                JSahibON::I64(1),
                JSahibON::try_f64(2.0).unwrap(),
                object([("a.b", JSahibON::Bool(true))]),
            ]),
            maybe_payload: None,
        },
        JsonDoc {
            id: 3,
            payload: JSahibON::String("third".to_owned()),
            maybe_payload: Some(object([("", JSahibON::U64(0))])),
        },
    ]
}

#[tokio::test]
async fn snapshot_postcard_entries_only_preserves_jsahibon_fields() {
    let source_docs = docs();
    let donor = Punnu::<JsonDoc>::builder().build();
    for doc in source_docs.clone() {
        donor.insert(doc).await.unwrap();
    }

    let bytes = donor.snapshot_postcard(SnapshotMode::EntriesOnly).unwrap();
    let restored = Punnu::<JsonDoc>::builder().build();
    let stats = restored.restore_postcard(&bytes).unwrap();

    assert_eq!(
        stats,
        PunnuRestoreStats {
            inserted: 3,
            updated: 0,
            removed: 0,
        }
    );
    for expected in &source_docs {
        assert_eq!(restored.get(&expected.id).as_deref(), Some(expected));
    }

    let restored_doc = restored.get(&1).unwrap();
    let JSahibON::Object(object) = &restored_doc.payload else {
        panic!("expected object payload");
    };
    assert_eq!(
        object
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        [
            "null",
            "bool",
            "i64",
            "u64",
            "f64",
            "string",
            "array",
            "object",
            "cafe\u{301}",
        ]
    );
}
