use sassi::__private::serde::{Deserialize, Serialize};
use sassi::Cacheable;

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.DuplicateWirePortable", wire_portable, wire_portable)]
struct DuplicateWirePortable {
    id: i64,
}

fn main() {}
