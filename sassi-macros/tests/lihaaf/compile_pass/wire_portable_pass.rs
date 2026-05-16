use sassi::__private::serde::{Deserialize, Serialize};
use sassi::{Cacheable, wire::SassiWire};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "sassi::__private::serde")]
struct PortableNewtype(String);

impl SassiWire for PortableNewtype {}

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.PortablePass", wire_portable)]
struct PortablePass {
    id: i64,
    name: String,
    score: Option<u64>,
    tags: Vec<String>,
    ranks: BTreeMap<String, u32>,
    labels: BTreeSet<String>,
    custom: PortableNewtype,
}

fn main() {
    let value = PortablePass {
        id: 1,
        name: "Ada".to_owned(),
        score: Some(10),
        tags: vec!["math".to_owned()],
        ranks: BTreeMap::from([("overall".to_owned(), 1)]),
        labels: BTreeSet::from(["featured".to_owned()]),
        custom: PortableNewtype("ok".to_owned()),
    };
    let bytes = sassi::wire::to_vec_portable(&value).unwrap();
    let _: PortablePass = sassi::wire::from_slice_portable(&bytes).unwrap();
}
