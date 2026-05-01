//! Cross-type orchestration for typed [`Punnu`](crate::punnu::Punnu)
//! pools.
//!
//! The orchestrator owns a map of typed pools plus the trait registry
//! used by `#[sassi::trait_impl]`. That combination lets callers ask
//! for every cached value implementing a shared trait without erasing
//! the typed pools themselves.

pub mod orchestrator;
pub mod trait_registry;

pub use orchestrator::Sassi;
pub use trait_registry::TraitRegistry;
