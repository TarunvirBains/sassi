use sassi::__private::serde::{Deserialize, Serialize};
use sassi::Cacheable;

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.FieldLevelWirePortable")]
struct FieldLevelWirePortable {
    id: i64,
    #[cacheable(wire_portable)]
    name: String,
}

fn main() {}
