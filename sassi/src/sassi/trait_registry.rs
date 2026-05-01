//! Trait implementation registry used by cross-type queries.
//!
//! Attribute macro expansions call the hidden raw registration
//! function at process startup. `Sassi::all_impl::<dyn Trait>()` then
//! filters those registrations by `TypeId`, asks each entry to collect
//! from the matching typed pool, and downcasts the returned typed
//! vector.

use crate::cacheable::Cacheable;
use crate::sassi::orchestrator::Sassi;
use std::any::{Any, TypeId};
use std::sync::{Arc, OnceLock, RwLock};

/// Type-erased collector emitted by `#[sassi::trait_impl]`.
///
/// The returned `Box<dyn Any>` contains a `Vec<Arc<dyn Trait>>` for
/// the trait named by [`TraitImplEntry::trait_type_id`]. Boxing the
/// vector keeps each collected item as a single `Arc<dyn Trait>` and
/// avoids unsafe pointer casts or double-`Arc` wrapping.
pub type CollectFn = fn(&Sassi) -> Box<dyn Any + Send + Sync>;

/// One registered `(model type, trait)` implementation.
///
/// Entries are append-only and process-global. A `Sassi` instance
/// still controls which model pools are present; the collector skips
/// an entry when the instance has no pool for its model type.
#[derive(Clone, Copy)]
pub struct TraitImplEntry {
    /// `TypeId` of the trait object, for example
    /// `TypeId::of::<dyn Nameable>()`.
    pub trait_type_id: TypeId,
    /// `TypeId` of the concrete model type stored in a `Punnu<T>`.
    pub model_type_id: TypeId,
    /// Type-erased collector for this `(T, Trait)` pair.
    pub collect_fn: CollectFn,
}

/// Handle used by [`Sassi`](crate::Sassi) to query trait
/// registrations.
///
/// The underlying storage is process-global because registrations
/// are emitted by proc-macro expansion at link time. Keeping this
/// lightweight handle on `Sassi` makes that dependency visible in the
/// orchestrator's shape without copying registration data per
/// instance.
#[derive(Clone, Copy, Default)]
pub struct TraitRegistry;

impl TraitRegistry {
    /// Construct a registry handle.
    pub fn new() -> Self {
        Self
    }

    /// Register a concrete model type for a trait using an emitted
    /// collector.
    ///
    /// Macro expansions call the raw free function instead; this
    /// method is public for tests and advanced integrations that
    /// generate their own collectors.
    pub fn register_trait_impl<T, Trait>(&mut self, collect_fn: CollectFn)
    where
        T: Cacheable + 'static,
        Trait: ?Sized + Send + Sync + 'static,
    {
        register_trait_impl_raw(TypeId::of::<Trait>(), TypeId::of::<T>(), collect_fn);
    }

    /// Collect all entries registered for `Trait` from this
    /// orchestrator.
    pub(crate) fn collect_for<Trait>(&self, sassi: &Sassi) -> Vec<Arc<Trait>>
    where
        Trait: ?Sized + Send + Sync + 'static,
    {
        let entries = registry()
            .read()
            .expect("Sassi trait registry lock poisoned");
        let mut out = Vec::new();

        for entry in entries
            .iter()
            .filter(|entry| entry.trait_type_id == TypeId::of::<Trait>())
        {
            let erased = (entry.collect_fn)(sassi);
            match erased.downcast::<Vec<Arc<Trait>>>() {
                Ok(mut typed) => out.append(&mut typed),
                Err(_) => {
                    tracing::warn!(
                        trait_type = ?entry.trait_type_id,
                        model_type = ?entry.model_type_id,
                        "sassi trait registry collector returned an unexpected payload"
                    );
                }
            }
        }

        out
    }
}

/// Register a trait implementation entry with raw type ids.
///
/// This is hidden from the public API surface and re-exported through
/// `sassi::__private` so proc-macro expansions can call it without
/// relying on module internals.
#[doc(hidden)]
pub fn register_trait_impl_raw(
    trait_type_id: TypeId,
    model_type_id: TypeId,
    collect_fn: CollectFn,
) {
    let mut entries = registry()
        .write()
        .expect("Sassi trait registry lock poisoned");
    if entries.iter().any(|entry| {
        entry.trait_type_id == trait_type_id
            && entry.model_type_id == model_type_id
            && std::ptr::fn_addr_eq(entry.collect_fn, collect_fn)
    }) {
        return;
    }
    entries.push(TraitImplEntry {
        trait_type_id,
        model_type_id,
        collect_fn,
    });
}

fn registry() -> &'static RwLock<Vec<TraitImplEntry>> {
    static REGISTRY: OnceLock<RwLock<Vec<TraitImplEntry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(Vec::new()))
}
