use sassi::__private::serde::{Deserialize, Serialize};
use sassi::{Cacheable, JSahibON};

#[derive(Debug, Clone, Serialize, Deserialize, Cacheable)]
#[serde(crate = "sassi::__private::serde")]
#[cacheable(type_name = "lihaaf.PortableJSahibONPass", wire_portable)]
struct PortableJSahibONPass {
    id: i64,
    payload: JSahibON,
    maybe_payload: Option<JSahibON>,
}

fn main() {
    let value = PortableJSahibONPass {
        id: 1,
        payload: JSahibON::Null,
        maybe_payload: Some(JSahibON::Bool(true)),
    };
    let bytes = sassi::wire::to_vec_portable(&value).unwrap();
    let _: PortableJSahibONPass = sassi::wire::from_slice_portable(&bytes).unwrap();
}
