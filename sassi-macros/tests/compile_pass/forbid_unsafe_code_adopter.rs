//! Adopter crate with `#![forbid(unsafe_code)]` must accept
//! `#[sassi::trait_impl]` without compile error.
//!
//! Proves that the `inventory::submit!` expansion does not surface
//! unsafe attribute syntax (`unsafe(link_section = ...)`) at the
//! adopter call site — the unsafe machinery is fully encapsulated
//! inside the `inventory` crate. Closes round-2 BLOCK-1 elevation
//! risk on the macro emission audit.

#![forbid(unsafe_code)]

use sassi::{Cacheable, Field};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct U {
    id: i64,
    name: String,
}

#[derive(Default)]
struct UFields {
    pub id: Field<U, i64>,
}

impl Cacheable for U {
    type Id = i64;
    type Fields = UFields;
    fn id(&self) -> i64 {
        self.id
    }
    fn fields() -> UFields {
        UFields {
            id: Field::new("id", |u| &u.id),
        }
    }
}

trait Nameable: Send + Sync + Any {
    fn name(&self) -> &str;
}

#[sassi::trait_impl]
impl Nameable for U {
    fn name(&self) -> &str {
        &self.name
    }
}

fn main() {
    let _: Vec<Arc<dyn Nameable>> = sassi::Sassi::new().all_impl::<dyn Nameable>();
}
