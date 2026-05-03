//! Single-flight fetch coalescing — the in-flight registry that
//! deduplicates concurrent `get_or_fetch` calls for the same id.
//!
//! Without coalescing, a hot key (for example, a user id queried from N
//! concurrent request handlers) generates N database round-trips on cold start.
//! With coalescing, exactly one fetch runs per id at any moment; subsequent
//! callers `await` the same future via [`futures::future::Shared`].
//!
//! # Cancellation contract (the four owner-loss cases)
//!
//! All four owner-loss cases behave deterministically:
//!
//! 1. **Originating caller dropped, peers polling.** [`Shared`] keeps
//!    the underlying fetch alive as long as ≥1 cloned handle exists.
//!    The longest-lived peer drives the work; the dropped originator
//!    simply stops receiving the result.
//! 2. **All awaiters drop simultaneously.** The fetch future is
//!    dropped; cancellation propagates through whatever the fetcher
//!    was awaiting (cancellation-safe primitives —
//!    `tokio_postgres::Client::query` is). The [`SlotGuard`] inside
//!    the future runs its `Drop` impl, which removes the registry
//!    entry. Subsequent calls retry from cold against an empty slot.
//! 3. **Fetcher panics.** [`Shared::poll`] propagates the panic to
//!    *every* awaiter. Each peer sees a
//!    [`crate::error::FetchError::FetcherPanic`]. Sassi wraps the
//!    fetcher in [`std::panic::AssertUnwindSafe`] +
//!    [`futures::FutureExt::catch_unwind`] so the panic is translated
//!    into a structured error variant rather than poisoning the
//!    runtime.
//! 4. **Fetcher exceeds caller-imposed deadline.** Punnu does not
//!    impose a deadline; consumers wrap with `tokio::time::timeout`
//!    at the call site (case 4 surfaces as case 1 from the registry's
//!    perspective).
//!
//! # Side-effect ordering — single L1 insert per fetch
//!
//! When a fetcher returns `Some(value)`, the L1 insert runs **inside**
//! the shared future body — exactly once per fetch — via the
//! `on_fetched` callback the caller supplies. Coalesced peers then
//! receive the canonical `Arc<T>` from the same Shared output; they
//! do not re-run the insert (which would otherwise multiply events,
//! TTL deadlines, and `OnConflict` policy evaluations across N peers).
//!
//! # Implementation
//!
//! The registry is an [`Arc<dashmap::DashMap<T::Id, Weak<Shared<...>>>>`].
//! Each caller holds a strong [`Arc<Shared<...>>`]; the DashMap holds
//! only a [`Weak`] reference. When all strong handles drop (case 2),
//! the Weak dangles and the underlying fetch future is dropped — real
//! cancellation, not just registry cleanup.
//!
//! Storing `Weak` rather than `Shared` directly is the key invariant —
//! it's what makes case 2 actually drop the fetch future. If the
//! DashMap held a `Shared` clone, that clone would be a strong
//! reference on the future and case 2 would degrade to "fetch runs to
//! completion with no observers, registry leaks until next call".
//!
//! Registry entries are cleaned up via [`SlotGuard`] — held inside the
//! Shared future body and run on `Drop` (whether triggered by
//! completion or all-awaiters cancellation). The guard uses
//! [`DashMap::remove_if`] with [`Weak::ptr_eq`] so it only removes the
//! entry when the registered Weak still points at our specific future
//! — a fresh fetch (case 2 → new caller) that has already replaced the
//! slot is left alone.

use crate::cacheable::Cacheable;
use crate::error::FetchError;
use dashmap::DashMap;
use futures::FutureExt;
use futures::future::Shared;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Weak};

/// Result of a single-flight fetch — `Ok(Some(arc))` on hit,
/// `Ok(None)` on "fetcher says doesn't exist", `Err(_)` on failure.
pub(crate) type FetchOutput<T> = Result<Option<Arc<T>>, FetchError>;

// `Result<Option<Arc<T>>, FetchError>` is not `Clone` because two of
// `FetchError`'s variants (`Backend(BackendError::Other(Box<dyn ...>))`
// and `Custom(Box<dyn ...>)`) hold `!Clone` boxed errors. `Shared`
// requires the inner Future's `Output: Clone` so every awaiter
// receives an independent copy. We can't make `BackendError` /
// `FetchError` clone without breaking type identity, so the registry
// stores a clone-friendly *render* of the error: structured variants
// stay structured, boxed errors render to their `Display` form. The
// originating caller receives a rendered copy too — that's the
// documented contract; coalesced peers see exactly what the first
// poller sees.
type SharedOutput<T> = Result<Option<Arc<T>>, FetchErrorClone>;

/// Clone-friendly render of [`FetchError`] used inside the
/// [`Shared`] future. Sealed against external construction — only
/// the single-flight path mints these.
#[derive(Debug, Clone)]
pub(crate) enum FetchErrorClone {
    /// Render of `FetchError::Backend(BackendError::NotFound)`.
    BackendNotFound,
    /// Render of `FetchError::Backend(BackendError::Serialization(_))`.
    BackendSerialization(String),
    /// Render of `FetchError::Backend(BackendError::Network(_))`.
    BackendNetwork(String),
    /// Render of `FetchError::Backend(BackendError::Other(_))` — the
    /// original boxed-error type identity is lost; the `Display`
    /// output round-trips. Documented contract — see module docs.
    BackendOtherRendered(String),
    /// Round-trip of `FetchError::Serialization`.
    Serialization(String),
    /// Round-trip of `FetchError::FetcherPanic`.
    FetcherPanic {
        /// `std::any::type_name::<T>()`.
        type_name: &'static str,
        /// Best-effort panic message.
        message: String,
    },
    /// Round-trip of `FetchError::IdentityMismatch`.
    IdentityMismatch {
        /// `std::any::type_name::<T>()`.
        type_name: &'static str,
    },
    /// Render of `FetchError::Custom` — `Display` output round-trips,
    /// type identity is lost.
    CustomRendered(String),
    /// Render of `FetchError::Insert(InsertError)`. The fetch path
    /// itself doesn't mint this variant; covered for symmetry if a
    /// fetcher chooses to return an `InsertError` it observed
    /// elsewhere.
    InsertRendered(String),
}

impl From<FetchErrorClone> for FetchError {
    fn from(value: FetchErrorClone) -> Self {
        use crate::error::BackendError;
        match value {
            FetchErrorClone::BackendNotFound => FetchError::Backend(BackendError::NotFound),
            FetchErrorClone::BackendSerialization(s) => {
                FetchError::Backend(BackendError::Serialization(s))
            }
            FetchErrorClone::BackendNetwork(s) => FetchError::Backend(BackendError::Network(s)),
            FetchErrorClone::BackendOtherRendered(s) => {
                FetchError::Backend(BackendError::Other(Box::new(RenderedError(s))))
            }
            FetchErrorClone::Serialization(s) => FetchError::Serialization(s),
            FetchErrorClone::FetcherPanic { type_name, message } => {
                FetchError::FetcherPanic { type_name, message }
            }
            FetchErrorClone::IdentityMismatch { type_name } => {
                FetchError::IdentityMismatch { type_name }
            }
            FetchErrorClone::CustomRendered(s) => FetchError::Custom(Box::new(RenderedError(s))),
            FetchErrorClone::InsertRendered(s) => {
                FetchError::Custom(Box::new(RenderedError(format!("insert during fetch: {s}"))))
            }
        }
    }
}

/// String-rendered carrier for the lossy clone path. The original
/// boxed type identity is lost; `Display` output round-trips.
#[derive(Debug)]
struct RenderedError(String);

impl std::fmt::Display for RenderedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RenderedError {}

pub(crate) fn into_clone(err: FetchError) -> FetchErrorClone {
    use crate::error::BackendError;
    match err {
        FetchError::Backend(BackendError::NotFound) => FetchErrorClone::BackendNotFound,
        FetchError::Backend(BackendError::Serialization(s)) => {
            FetchErrorClone::BackendSerialization(s)
        }
        FetchError::Backend(BackendError::Network(s)) => FetchErrorClone::BackendNetwork(s),
        #[cfg(feature = "serde")]
        FetchError::Backend(BackendError::WireFormat(e)) => {
            FetchErrorClone::BackendSerialization(format!("{e}"))
        }
        FetchError::Backend(BackendError::Other(e)) => {
            FetchErrorClone::BackendOtherRendered(format!("{e}"))
        }
        FetchError::Serialization(s) => FetchErrorClone::Serialization(s),
        FetchError::FetcherPanic { type_name, message } => {
            FetchErrorClone::FetcherPanic { type_name, message }
        }
        FetchError::IdentityMismatch { type_name } => {
            FetchErrorClone::IdentityMismatch { type_name }
        }
        FetchError::Custom(e) => FetchErrorClone::CustomRendered(format!("{e}")),
        FetchError::Insert(e) => FetchErrorClone::InsertRendered(format!("{e}")),
    }
}

/// The shared fetch payload. `Pin<Arc<...>>` so the Shared future is
/// allocated once and clonable cheaply; the inner Shared drives the
/// underlying fetcher.
type SharedFetchFuture<T> = Shared<Pin<Box<dyn Future<Output = SharedOutput<T>> + Send>>>;

/// Strong handle to an in-flight fetch. Cloning this attaches another
/// awaiter; dropping every clone allows the underlying fetch future
/// to be dropped (case 2 of the cancellation contract).
type StrongFetch<T> = Arc<SharedFetchFuture<T>>;

/// Weak handle stored in the registry. Upgrade to a `StrongFetch`
/// to attach as another awaiter; if the upgrade fails, the previous
/// fetch was abandoned and a new caller should register fresh.
type WeakFetch<T> = Weak<SharedFetchFuture<T>>;

/// Drop guard that removes the registry entry when the wrapped Shared
/// future is dropped (whether via completion or all-awaiters
/// cancellation). Held *inside* the boxed future so its lifetime is
/// pegged to the future's; when the future drops, the guard runs.
///
/// The race-safety guarantee comes from [`DashMap::remove_if`]: the
/// removal is conditional on the entry's `Weak` still pointing at our
/// specific future. A fresh fetch (case 2 → new caller) that has
/// already replaced the slot has a different `Weak` and is left
/// alone.
struct SlotGuard<T: Cacheable> {
    pending: Arc<DashMap<T::Id, WeakFetch<T>>>,
    id: T::Id,
    self_weak: WeakFetch<T>,
}

impl<T: Cacheable> Drop for SlotGuard<T> {
    fn drop(&mut self) {
        // `remove_if` runs the predicate under the shard's write
        // lock; the entry is removed iff the current Weak `ptr_eq`s
        // ours. No TOCTOU window between check and remove.
        self.pending.remove_if(&self.id, |_k, current_weak| {
            Weak::ptr_eq(current_weak, &self.self_weak)
        });
    }
}

/// In-flight fetch registry.
///
/// `pub(crate)` — wired through [`crate::punnu::pool::PunnuInner`] so
/// `Punnu::get_or_fetch` can route through it without exposing the
/// registry shape to consumers.
pub(crate) struct InFlightRegistry<T: Cacheable> {
    pending: Arc<DashMap<T::Id, WeakFetch<T>>>,
}

impl<T: Cacheable> InFlightRegistry<T> {
    /// Empty registry. Constructed once per `PunnuInner<T>`.
    pub(crate) fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::new()),
        }
    }

    /// Run `fetcher` exactly once across concurrent calls for the same
    /// `id`, then run `on_fetched` exactly once on the resulting
    /// `Arc<T>` to install it into L1. Subsequent in-flight callers
    /// share the same Shared future and observe the same canonical
    /// result; they do **not** re-run the insert (which would
    /// otherwise multiply events / TTL deadlines / OnConflict
    /// evaluations across N peers).
    ///
    /// `on_fetched` returns the canonical `Arc<T>` — usually the same
    /// `Arc<T>` it received, but consumers may swap it for the
    /// already-cached value when handling `OnConflict::Reject`
    /// conflicts.
    ///
    /// Cancellation contract is documented at the module level.
    pub(crate) async fn get_or_fetch<F, Fut, OnFetched, OnFetchedFut>(
        &self,
        id: &T::Id,
        fetcher: F,
        on_fetched: OnFetched,
    ) -> FetchOutput<T>
    where
        F: FnOnce(T::Id) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<T>, FetchError>> + Send + 'static,
        OnFetched: FnOnce(T::Id, Arc<T>) -> OnFetchedFut + Send + 'static,
        OnFetchedFut: Future<Output = Arc<T>> + Send + 'static,
    {
        // Atomic registration via the entry API: probe + insert under
        // a single shard lock so two concurrent callers can't both
        // think they're the originator. Re-asserting the lookup under
        // the entry API closes the read-then-mutate race window.
        let strong: StrongFetch<T> = match self.pending.entry(id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => match e.get().upgrade() {
                Some(strong) => strong,
                None => {
                    // Stale entry — the previous fetch was abandoned
                    // (case 2: all awaiters dropped, but the SlotGuard
                    // hasn't run yet OR ran but a concurrent caller
                    // re-inserted between our `entry()` and the guard's
                    // `remove_if`). Replace it with a fresh fetch
                    // under the same shard lock.
                    let strong = build_fetch::<T, _, _, _, _>(
                        id.clone(),
                        fetcher,
                        on_fetched,
                        &self.pending,
                    );
                    e.insert(Arc::downgrade(&strong));
                    strong
                }
            },
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let strong =
                    build_fetch::<T, _, _, _, _>(id.clone(), fetcher, on_fetched, &self.pending);
                e.insert(Arc::downgrade(&strong));
                strong
            }
        };

        // Clone the inner Shared so we can `.await` it. `Shared` is
        // itself ref-counted; this is cheap. The strong `Arc` we
        // hold (`_strong_holder` below) keeps the underlying fetch
        // future alive until either (a) it completes or (b) every
        // awaiter drops — at which point the strong count of the
        // `Arc` reaches zero, the inner Shared is dropped, the boxed
        // future is dropped, and the SlotGuard inside it runs Drop
        // and clears the registry entry. Real cancellation, no
        // background-task leak.
        let shared = (*strong).clone();
        let _strong_holder = strong;

        let out: SharedOutput<T> = shared.await;
        out.map_err(FetchError::from)
    }
}

/// Build a fresh fetch future for the given id + fetcher closure +
/// on_fetched callback, wrap it in [`Shared`], and return the
/// `Arc<Shared<...>>` strong handle.
///
/// Uses [`Arc::new_cyclic`] so the [`SlotGuard`] inside the future
/// body can capture its own `Weak<SharedFetchFuture<T>>` for the
/// race-safe `remove_if`. The cyclic construction is sound because
/// the closure is sync (Shared::new is sync; the async work is inside
/// the boxed future, polled later).
fn build_fetch<T, F, Fut, OnFetched, OnFetchedFut>(
    id: T::Id,
    fetcher: F,
    on_fetched: OnFetched,
    pending: &Arc<DashMap<T::Id, WeakFetch<T>>>,
) -> StrongFetch<T>
where
    T: Cacheable,
    F: FnOnce(T::Id) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Option<T>, FetchError>> + Send + 'static,
    OnFetched: FnOnce(T::Id, Arc<T>) -> OnFetchedFut + Send + 'static,
    OnFetchedFut: Future<Output = Arc<T>> + Send + 'static,
{
    let pending = pending.clone();
    let type_name = std::any::type_name::<T>();
    let id_for_guard = id.clone();
    let id_for_fetch = id.clone();
    let id_for_on_fetched = id;
    let pending_for_guard = pending;

    Arc::new_cyclic(move |self_weak: &WeakFetch<T>| {
        let self_weak_for_guard = self_weak.clone();
        // `AssertUnwindSafe` + `catch_unwind` translate a panicking
        // fetcher into a structured `FetcherPanic` variant rather than
        // poisoning the Shared future. `Shared` already broadcasts
        // panics, but the broadcast surfaces as a `BroadcastedPanic` the
        // consumer can't pattern-match on; sassi promises a structured
        // error type. `AssertUnwindSafe` is sound because the fetcher's
        // borrow does not escape this future — any state the fetcher
        // mutated before panicking is owned by it (the FnOnce closure)
        // and is dropped along with the unwound stack.
        let inner: Pin<Box<dyn Future<Output = SharedOutput<T>> + Send>> = Box::pin(async move {
            // Held for the whole future body. Drops on completion OR
            // on all-awaiters cancellation; either way, the registry
            // slot is cleared.
            let _slot_guard = SlotGuard {
                pending: pending_for_guard,
                id: id_for_guard,
                self_weak: self_weak_for_guard,
            };

            let result = AssertUnwindSafe(async move { fetcher(id_for_fetch).await })
                .catch_unwind()
                .await;
            match result {
                Ok(Ok(Some(value))) => {
                    if value.id() != id_for_on_fetched {
                        return Err(FetchErrorClone::IdentityMismatch { type_name });
                    }
                    let arc = Arc::new(value);
                    // Run the L1 insert exactly once, here, before
                    // any awaiter receives the canonical Arc. The
                    // callback returns the canonical value (the same
                    // Arc on success, or the already-cached Arc on
                    // OnConflict::Reject collision).
                    let canonical = on_fetched(id_for_on_fetched, arc).await;
                    Ok(Some(canonical))
                }
                Ok(Ok(None)) => Ok(None),
                Ok(Err(e)) => Err(into_clone(e)),
                Err(panic_payload) => {
                    let message = if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else {
                        String::new()
                    };
                    Err(FetchErrorClone::FetcherPanic { type_name, message })
                }
            }
        });
        inner.shared()
    })
}
