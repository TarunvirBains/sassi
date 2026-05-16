#![cfg(feature = "serde-json-bridge")]

use sassi::{JObject, JSahibON};

#[test]
fn serde_json_bridge_roundtrips_valid_portable_values() {
    let json = serde_json::json!({
        "null": null,
        "bool": true,
        "i64": -1,
        "u64": 9223372036854775808_u64,
        "f64": 1.25,
        "string": "hello",
        "array": [1, true, "x"]
    });

    let portable = JSahibON::try_from(json.clone()).unwrap();
    let back: serde_json::Value = portable.into();

    assert_eq!(back, json);
}

#[test]
fn serde_json_bridge_projects_serializable_types_and_typed_values() {
    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
    struct Payload {
        name: String,
        count: u64,
    }

    let payload = Payload {
        name: "Ada".to_owned(),
        count: 3,
    };
    let portable = JSahibON::try_from_serializable(&payload).unwrap();
    let typed: Payload = portable.try_into_typed().unwrap();

    assert_eq!(typed, payload);
}

#[test]
fn jsahibon_to_serde_json_preserves_object_iteration_order() {
    let value = JSahibON::Object(JObject::from_entries([
        ("b".to_owned(), JSahibON::Bool(true)),
        ("a".to_owned(), JSahibON::I64(1)),
    ]));

    let json: serde_json::Value = value.into();
    let keys = json
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(keys, ["b", "a"]);
}
