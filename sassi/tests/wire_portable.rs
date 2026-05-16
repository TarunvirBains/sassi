#![cfg(feature = "serde")]

use sassi::{Cacheable, JSahibON};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Cacheable)]
#[cacheable(type_name = "sassi.test.PortableWireEntry", wire_portable)]
struct PortableWireEntry {
    id: i64,
    name: String,
    metadata: BTreeMap<String, Option<JSahibON>>,
}

#[test]
fn portable_wire_helpers_are_byte_identical_to_loose_helpers() {
    let entry = PortableWireEntry {
        id: 7,
        name: "Ada".to_owned(),
        metadata: BTreeMap::from([(
            "role".to_owned(),
            Some(JSahibON::String("admin".to_owned())),
        )]),
    };

    let loose = sassi::wire::to_vec(&entry).unwrap();
    let portable = sassi::wire::to_vec_portable(&entry).unwrap();
    let decoded = sassi::wire::from_slice_portable::<PortableWireEntry>(&portable).unwrap();

    assert_eq!(portable, loose);
    assert_eq!(decoded, entry);
}
