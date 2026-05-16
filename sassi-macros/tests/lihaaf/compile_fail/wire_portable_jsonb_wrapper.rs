use sassi::__private::serde::{Deserialize, Serialize};
use sassi::Cacheable;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "sassi::__private::serde")]
struct Jsonb<T>(T);

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.JsonbWrapperRejected", wire_portable)]
struct JsonbWrapperRejected {
    id: i64,
    payload: Jsonb<String>,
}

fn main() {}
