// compile_fail: the TraitImpl marker has a sealed supertrait. A plain
// hand-written `impl TraitImpl<dyn Trait> for Type {}` is rejected at
// compile time, so a type that skips `#[sassi::trait_impl]` cannot be
// passed to filter_impl. (Reaching into #[doc(hidden)] sassi::__private
// to forge the supertrait by hand is possible but warranty-voiding.)

use sassi::{Cacheable, TraitImpl, punnu::PunnuScope};

trait IsVehicle: Send + Sync {}

#[derive(Clone)]
struct Car {
    id: u32,
}

#[derive(Default)]
struct EmptyFields;

impl Cacheable for Car {
    type Id = u32;
    type Fields = EmptyFields;
    fn id(&self) -> u32 {
        self.id
    }
    fn fields() -> EmptyFields {
        EmptyFields
    }
}

// Forged registration: no #[sassi::trait_impl] anywhere. This impl must be
// rejected because TraitImpl's sealed supertrait is unreachable here.
impl TraitImpl<dyn IsVehicle> for Car {}

fn uses_it(scope: PunnuScope<Car>) {
    let _ = scope.filter_impl::<dyn IsVehicle>();
}

fn main() {}
