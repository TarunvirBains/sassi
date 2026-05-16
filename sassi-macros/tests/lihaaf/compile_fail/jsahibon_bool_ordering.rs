use sassi::{Cacheable, JSahibON};

#[derive(Debug, Clone, Cacheable)]
#[cacheable(type_name = "lihaaf.JSahibONBoolOrdering")]
struct Event {
    id: i64,
    payload: JSahibON,
}

fn main() {
    let _ = Event::fields()
        .payload
        .jsahibon()
        .value::<bool>()
        .gt(true);
}
