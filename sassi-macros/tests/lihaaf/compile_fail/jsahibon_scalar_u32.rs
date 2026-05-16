use sassi::{Cacheable, JSahibON};

#[derive(Debug, Clone, Cacheable)]
#[cacheable(type_name = "lihaaf.JSahibONScalarU32")]
struct Event {
    id: i64,
    payload: JSahibON,
}

fn main() {
    let _ = Event::fields().payload.jsahibon().value::<u32>();
}
