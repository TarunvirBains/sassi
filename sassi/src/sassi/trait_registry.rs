//! Trait implementation registry used by cross-type queries.
//!
//! Each `#[sassi::trait_impl]` expansion submits one
//! [`TraitImplEntry`] to the global [`inventory`] registry at link
//! time. `Sassi::all_impl::<dyn Trait>()` then walks the registry,
//! filters by `TypeId`, and asks each entry to collect from the
//! matching typed pool.
//!
//! The registry is process-global because trait implementations are
//! emitted by proc-macro expansion at link time. A given [`Sassi`]
//! instance still controls which model pools are present; the
//! collector skips an entry whose model type has no registered pool.
//!
//! # Why `inventory` instead of a hand-rolled link section
//!
//! `inventory` encapsulates the platform-specific link-section
//! attribute (`.init_array` / `__DATA,__mod_init_func` /
//! `.CRT$XCU`) inside its own crate. Adopter crates that set
//! `#![forbid(unsafe_code)]` are not rejected because the unsafe
//! attribute syntax never appears in the expansion of
//! `#[sassi::trait_impl]`. `inventory` also has documented support
//! for `wasm32-unknown-unknown`, so registration fires automatically
//! on every supported sassi target.
//!
//! Macro expansion routes through [`crate::__private::inventory`] so
//! emitted code does not depend on the workspace's exact
//! `inventory` version reaching adopter `Cargo.lock` files.
//!
//! # WASM runtime registration
//!
//! `cargo build --target wasm32-unknown-unknown` finishes clean with
//! the inventory-based registry, and `inventory` has documented
//! support for the wasm32 startup-init slot. Full *runtime-test*
//! coverage on `wasm32-unknown-unknown` requires the
//! `wasm-bindgen-test` runner, which is tracked by sassi GitHub
//! issue #3 (per-test wasm execution + full multi-target CI matrix).
//! Until issue #3 lands, runtime registration on wasm32 is asserted
//! transitively via inventory's documented WASM support (since 2019)
//! rather than a direct sassi-side wasm-bindgen-test.

use crate::sassi::orchestrator::Sassi;
use std::any::{Any, TypeId};
use std::collections::HashSet;
use std::sync::Arc;

/// Type-erased collector emitted by `#[sassi::trait_impl]`.
///
/// The returned `Box<dyn Any>` contains a `Vec<Arc<dyn Trait>>` for
/// the trait named by [`TraitImplEntry::trait_type_id`]. Boxing the
/// vector keeps each collected item as a single `Arc<dyn Trait>` and
/// avoids `unsafe` pointer casts or double-`Arc` wrapping.
pub type CollectFn = fn(&Sassi) -> Box<dyn Any + Send + Sync>;

/// One registered `(model type, trait)` implementation.
///
/// `inventory` hands out a static slice of these at process startup;
/// see [`inventory::submit!`] for the registration macro the
/// `#[sassi::trait_impl]` expansion calls into.
pub struct TraitImplEntry {
    /// `TypeId` of the trait object, for example
    /// `TypeId::of::<dyn Nameable>()`.
    pub trait_type_id: TypeId,
    /// `TypeId` of the concrete model type stored in a `Punnu<T>`.
    pub model_type_id: TypeId,
    /// Type-erased collector for this `(T, Trait)` pair.
    pub collect_fn: CollectFn,
}

inventory::collect!(TraitImplEntry);

/// Handle used by [`Sassi`](crate::Sassi) to query trait
/// registrations.
///
/// The underlying storage is the process-global [`inventory`]
/// registry, which is populated at link time by every
/// `#[sassi::trait_impl]` expansion. Keeping this lightweight handle
/// on `Sassi` makes the dependency visible in the orchestrator's
/// shape without copying registration data per instance.
#[derive(Clone, Copy, Default)]
pub struct TraitRegistry;

impl TraitRegistry {
    /// Construct a registry handle.
    pub fn new() -> Self {
        Self
    }

    /// Collect all entries registered for `Trait` from this
    /// orchestrator.
    ///
    /// `Trait` must be `Send + Sync + 'static` so the collector's
    /// `Vec<Arc<dyn Trait>>` payload satisfies the [`Any`] bound on
    /// type-erased downcast. The macro surfaces this requirement at
    /// the call site via the constraint on
    /// [`Sassi::all_impl`](crate::Sassi::all_impl); adopters who
    /// declare a trait without those bounds get a compile-time error
    /// at the macro invocation, not at runtime.
    pub(crate) fn collect_for<Trait>(&self, sassi: &Sassi) -> Vec<Arc<Trait>>
    where
        Trait: ?Sized + Send + Sync + 'static,
    {
        let mut out = Vec::new();
        let target = TypeId::of::<Trait>();
        let mut seen: HashSet<(TypeId, TypeId)> = HashSet::new();
        for entry in inventory::iter::<TraitImplEntry>() {
            if entry.trait_type_id != target {
                continue;
            }
            if !seen.insert((entry.trait_type_id, entry.model_type_id)) {
                tracing::debug!(
                    "sassi: dedup — skipping duplicate registration for ({:?}, {:?})",
                    entry.trait_type_id,
                    entry.model_type_id
                );
                continue;
            }
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
