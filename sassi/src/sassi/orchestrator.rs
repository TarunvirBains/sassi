//! [`Sassi`] - owner of typed pools and cross-type trait queries.
//!
//! Each cached model type keeps its own [`Punnu`](crate::punnu::Punnu)
//! instance. `Sassi` stores those pools behind `TypeId` and delegates
//! trait-object collection to [`TraitRegistry`](super::TraitRegistry),
//! which is populated by `#[sassi::trait_impl]` expansions.

use crate::cacheable::Cacheable;
use crate::punnu::Punnu;
use crate::sassi::trait_registry::TraitRegistry;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Cross-type orchestrator for `Punnu<T>` pools.
///
/// `Sassi` keeps the pools typed at the edge: callers register and
/// retrieve `Arc<Punnu<T>>` by concrete `T`, while
/// [`all_impl`](Self::all_impl) walks the trait registry to collect
/// `Arc<dyn Trait>` values across every registered pool whose model
/// type advertised that trait.
pub struct Sassi {
    pools: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    trait_registry: TraitRegistry,
}

impl Sassi {
    /// Construct an empty orchestrator.
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            trait_registry: TraitRegistry::new(),
        }
    }

    /// Register a typed pool under its model `TypeId`.
    ///
    /// Re-registering the same model type replaces the previous pool.
    /// That makes test setup and application bootstrapping explicit:
    /// the latest registration is the pool that cross-type queries
    /// observe.
    pub fn register<T>(&mut self, pool: Arc<Punnu<T>>)
    where
        T: Cacheable + 'static,
    {
        self.pools
            .write()
            .expect("Sassi pool registry lock poisoned")
            .insert(TypeId::of::<T>(), pool);
    }

    /// Retrieve the registered pool for `T`, if any.
    ///
    /// The returned `Arc` is the same handle that was registered, so
    /// identity-sensitive callers can compare it with
    /// [`Arc::ptr_eq`].
    pub fn pool<T>(&self) -> Option<Arc<Punnu<T>>>
    where
        T: Cacheable + 'static,
    {
        let erased = self
            .pools
            .read()
            .expect("Sassi pool registry lock poisoned")
            .get(&TypeId::of::<T>())
            .cloned()?;
        Arc::downcast::<Punnu<T>>(erased).ok()
    }

    /// Collect cached entries across every registered model type that
    /// implements `Trait`.
    ///
    /// The trait implementation pairs are registered by
    /// `#[sassi::trait_impl]`. Missing pools are skipped: registering
    /// a trait implementation says the model type can participate,
    /// but a particular `Sassi` instance still decides which pools it
    /// owns.
    ///
    /// # Trait bounds
    ///
    /// `Trait` must satisfy `Send + Sync + 'static`. The bound is
    /// load-bearing — the registry's collector boxes its typed
    /// `Vec<Arc<dyn Trait>>` payload as
    /// `Box<dyn Any + Send + Sync>` for type erasure across the
    /// inventory boundary, and `Any` requires `'static`. Adopters
    /// who declare a trait without those bounds receive a
    /// compile-time error at the `Sassi::all_impl` call, not at
    /// runtime.
    pub fn all_impl<Trait>(&self) -> Vec<Arc<Trait>>
    where
        Trait: ?Sized + Send + Sync + 'static,
    {
        self.trait_registry.collect_for::<Trait>(self)
    }
}

impl Default for Sassi {
    fn default() -> Self {
        Self::new()
    }
}
