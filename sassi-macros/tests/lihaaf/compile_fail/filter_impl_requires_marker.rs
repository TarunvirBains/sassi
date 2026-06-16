use sassi::{Cacheable, punnu::PunnuScope};

trait IsVehicle: Send + Sync {}

#[derive(Clone)]
struct Car { id: u32 }

#[derive(Default)]
struct EmptyFields;

impl Cacheable for Car {
    type Id = u32;
    type Fields = EmptyFields;
    fn id(&self) -> u32 { self.id }
    fn fields() -> EmptyFields { EmptyFields }
}

fn check(scope: PunnuScope<Car>) {
    let _ = scope.filter_impl::<dyn IsVehicle>();
}
