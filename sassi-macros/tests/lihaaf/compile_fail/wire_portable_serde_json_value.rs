use sassi::__private::serde::{Deserialize, Serialize};
use sassi::Cacheable;
use sassi::__private::serde_json;

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.SerdeJsonValueRejected", wire_portable)]
struct SerdeJsonValueRejected {
    id: i64,
    payload: serde_json::Value,
}

fn main() {}
