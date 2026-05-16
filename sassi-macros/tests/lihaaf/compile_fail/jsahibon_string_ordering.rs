use sassi::{Cacheable, JSahibON};

#[derive(Debug, Clone, Cacheable)]
#[cacheable(type_name = "lihaaf.JSahibONStringOrdering")]
struct Event {
    id: i64,
    payload: JSahibON,
}

fn main() {
    let _ = Event::fields()
        .payload
        .jsahibon()
        .value::<String>()
        .gt("Ada".to_owned());
}
