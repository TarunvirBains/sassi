use sassi::__private::serde::{Deserialize, Serialize};
use sassi::Cacheable;

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.LooseOnly")]
struct LooseOnly {
    id: i64,
    name: String,
}

fn main() {
    let value = LooseOnly {
        id: 1,
        name: "Ada".to_owned(),
    };
    let _ = sassi::wire::to_vec_portable(&value);
}
