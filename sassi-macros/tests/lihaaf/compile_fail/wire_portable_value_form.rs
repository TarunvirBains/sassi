use sassi::__private::serde::{Deserialize, Serialize};
use sassi::Cacheable;

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.WirePortableValueForm", wire_portable = true)]
struct WirePortableValueForm {
    id: i64,
}

fn main() {}
