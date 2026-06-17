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
//! `#[sassi::trait_impl]`. `inventory` also documents support for
//! `wasm32-unknown-unknown`; Sassi's current release gate verifies the
//! WASM compile path and runs direct wasm-bindgen runtime tests for the
//! `runtime-wasm` executor.
//!
//! Macro expansion routes through [`crate::__private::inventory`] so
//! emitted code does not depend on the workspace's exact
//! `inventory` version reaching adopter `Cargo.lock` files.
//!
//! # WASM runtime registration
//!
//! `cargo build --target wasm32-unknown-unknown` finishes clean with
//! the inventory-based registry, and `inventory` documents support for
//! the wasm32 startup-init slot. The current CI gate also runs the
//! `runtime-wasm` integration suite through `wasm-bindgen-test`; trait
//! registry runtime coverage remains indirect through compile and link
//! coverage.

use crate::sassi::orchestrator::Sassi;
use std::any::{Any, TypeId};
use std::collections::HashSet;
use std::sync::Arc;

/// Module-private sealing trait. Only `#[sassi::trait_impl]` can emit an
/// `impl Sealed<dyn Trait> for Type`, so adopters cannot hand-write a
/// `TraitImpl<dyn Trait>` impl to forge cross-type registration. The
/// `__` prefix marks this as internal plumbing, not adopter API.
#[doc(hidden)]
pub(crate) mod __sealed {
    /// Sealed supertrait of [`super::TraitImpl`], parameterized by the
    /// trait object so a type sealed for `dyn A` is not thereby sealed
    /// for `dyn B`. Re-exported as `crate::__private::Sealed` for the
    /// macro; never named by adopters.
    pub trait Sealed<Trait: ?Sized> {}
}

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

/// Compile-time marker: `T` has a registered implementation of `Trait`
/// via `#[sassi::trait_impl]`.
///
/// # What
/// One marker impl is emitted per `(Type, Trait)` pair the
/// `#[sassi::trait_impl]` attribute macro processes. The trait carries no
/// methods and no runtime cost — it exists purely so method bounds (most
/// notably [`PunnuScope::filter_impl`](crate::punnu::PunnuScope::filter_impl))
/// can require, at compile time, that the cached type participates in a
/// given trait's cross-type registry. The marker has a sealed supertrait — a plain hand-written `impl TraitImpl<dyn Trait> for Type {}` is rejected at compile time; registration goes through `#[sassi::trait_impl]`. (Reaching directly into `#[doc(hidden)] sassi::__private` is possible but explicitly warranty-voiding, and mirrors the existing treatment of `TraitImplEntry`.)
///
/// # Why
/// [`Sassi::all_impl`](crate::Sassi::all_impl) answers the *cross-type*
/// question ("every cached value implementing `Trait`, across pools") and
/// returns erased `Arc<dyn Trait>`. `filter_impl` answers the *single-type*
/// question ("narrow this `Punnu<T>` scope to entries that implement
/// `Trait`") while keeping the concrete `Arc<T>`. Because a `Punnu<T>` holds
/// exactly one concrete type, the marker bound proves at compile time that
/// the whole pool qualifies — turning the narrowing into a guarantee the
/// type system enforces rather than a runtime predicate.
///
/// # How
/// Adopters never write this impl by hand; `#[sassi::trait_impl]` emits it:
/// ```ignore
/// #[sassi::trait_impl]
/// impl Searchable for Vehicle { /* ... */ }
/// // expands to: impl Searchable for Vehicle { ... }
/// //          +  impl ::sassi::TraitImpl<dyn Searchable> for Vehicle {}
/// //          +  the sealed witness + inventory::submit!(TraitImplEntry { ... });
/// ```
///
/// # Where
/// Reach for the bound only in generic code that must require trait-registry
/// participation (e.g. when forwarding to `filter_impl` from your own helper).
/// Most call sites use `filter_impl` directly and never name `TraitImpl`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not registered as an implementation of `{Trait}`",
    note = "apply `#[sassi::trait_impl]` to the `impl {Trait} for {Self}` block"
)]
pub trait TraitImpl<Trait: ?Sized>: __sealed::Sealed<Trait> {}

/// Handle used by [`Sassi`] to query trait
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

#[cfg(test)]
mod marker_tests {
    use super::TraitImpl;

    trait Demo: Send + Sync {}

    struct Widget;
    impl Demo for Widget {}
    // Stand-in witnesses matching what #[sassi::trait_impl] now emits:
    // both the TraitImpl impl and the sealed supertrait witness.
    impl super::__sealed::Sealed<dyn Demo> for Widget {}
    // Hand-written marker impl standing in for what the macro will emit.
    impl TraitImpl<dyn Demo> for Widget {}

    fn requires_marker<T: TraitImpl<dyn Demo>>() {}

    #[test]
    fn marker_bound_resolves_for_registered_pair() {
        requires_marker::<Widget>();
    }
}
