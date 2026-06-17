use sassi::{Cacheable, MemQ, Punnu};
use std::sync::Arc;

#[allow(dead_code)]
trait IsVehicle: Send + Sync {
    fn wheels(&self) -> u8;
}

#[derive(Debug, Clone)]
struct Car {
    id: i64,
}

#[derive(Default)]
struct EmptyFields;

impl Cacheable for Car {
    type Id = i64;
    type Fields = EmptyFields;
    fn id(&self) -> i64 {
        self.id
    }
    fn fields() -> EmptyFields {
        EmptyFields
    }
}

#[sassi::trait_impl]
impl IsVehicle for Car {
    fn wheels(&self) -> u8 {
        4
    }
}

#[tokio::test]
async fn filter_impl_returns_all_entries_of_implementing_type() {
    let cars = Punnu::<Car>::builder().build();
    cars.insert(Car { id: 1 }).await.unwrap();
    cars.insert(Car { id: 2 }).await.unwrap();

    let got: Vec<Arc<Car>> = cars
        .scope(Vec::<MemQ<Car>>::new())
        .filter_impl::<dyn IsVehicle>()
        .collect();

    let mut ids: Vec<i64> = got.iter().map(|c| c.id).collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test]
async fn filter_impl_composes_with_other_filters() {
    let cars = Punnu::<Car>::builder().build();
    cars.insert(Car { id: 1 }).await.unwrap();
    cars.insert(Car { id: 2 }).await.unwrap();

    let got: Vec<Arc<Car>> = cars
        .scope(Vec::<MemQ<Car>>::new())
        .filter_impl::<dyn IsVehicle>()
        .filter_closure(|c| c.id == 2)
        .collect();

    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, 2);
}
