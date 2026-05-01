//! Single-flight fetch coalescing — the in-flight registry that
//! deduplicates concurrent `get_or_fetch` calls for the same id.
//!
//! Spec §3.5.1 — without coalescing, a hot key (e.g., a user-id queried
//! from N concurrent request handlers) generates N database
//! round-trips on cold-start. With coalescing, exactly one fetch per
//! id at any moment; subsequent callers `await` the same future via
//! [`futures::future::Shared`].
//!
//! # Cancellation contract (the four owner-loss cases)
//!
//! From spec §3.5.1, all four must behave deterministically:
//!
//! 1. **Originating caller dropped, peers polling.** [`Shared`] keeps
//!    the underlying fetch alive as long as ≥1 cloned handle exists.
//!    The longest-lived peer drives the work; the dropped originator
//!    simply stops receiving the result.
//! 2. **All awaiters drop simultaneously.** The fetch future is
//!    dropped; cancellation propagates through whatever the fetcher
//!    was awaiting (cancellation-safe primitives —
//!    `tokio_postgres::Client::query` is). Subsequent calls retry
//!    from cold.
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
//! # Implementation
//!
//! The registry is a [`dashmap::DashMap<T::Id, Weak<Shared<...>>>`].
//! Each caller holds a strong [`Arc<Shared<...>>`]; the DashMap holds
//! only a [`Weak`] reference. When all strong handles drop (case 2),
//! the Weak dangles and the underlying fetch future is dropped — real
//! cancellation, not just registry cleanup. A new caller for the same
//! id observes the dead Weak (`upgrade()` returns `None`) and starts
//! a fresh fetch. The dead entry is replaced atomically under the
//! [`dashmap::mapref::entry::Entry`] API.
//!
//! Storing `Weak` rather than `Shared` directly is the key invariant —
//! it's what makes case 2 actually drop the fetch future. If the
//! DashMap held a `Shared` clone, that clone would be a strong
//! reference on the future and case 2 would degrade to "fetch runs to
//! completion with no observers, registry leaks until next call".

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

fn into_clone(err: FetchError) -> FetchErrorClone {
    use crate::error::BackendError;
    match err {
        FetchError::Backend(BackendError::NotFound) => FetchErrorClone::BackendNotFound,
        FetchError::Backend(BackendError::Serialization(s)) => {
            FetchErrorClone::BackendSerialization(s)
        }
        FetchError::Backend(BackendError::Network(s)) => FetchErrorClone::BackendNetwork(s),
        FetchError::Backend(BackendError::Other(e)) => {
            FetchErrorClone::BackendOtherRendered(format!("{e}"))
        }
        FetchError::Serialization(s) => FetchErrorClone::Serialization(s),
        FetchError::FetcherPanic { type_name, message } => {
            FetchErrorClone::FetcherPanic { type_name, message }
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

/// In-flight fetch registry.
///
/// `pub(crate)` — wired through [`crate::punnu::pool::PunnuInner`] so
/// `Punnu::get_or_fetch` can route through it without exposing the
/// registry shape to consumers.
pub(crate) struct InFlightRegistry<T: Cacheable> {
    pending: DashMap<T::Id, WeakFetch<T>>,
}

impl<T: Cacheable> InFlightRegistry<T> {
    /// Empty registry. Constructed once per `PunnuInner<T>`.
    pub(crate) fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    /// Run `fetcher` exactly once across concurrent calls for the same
    /// `id`. Subsequent in-flight callers share the same Shared
    /// future and observe the same result.
    ///
    /// Cancellation contract is documented at the module level.
    pub(crate) async fn get_or_fetch<F, Fut>(&self, id: &T::Id, fetcher: F) -> FetchOutput<T>
    where
        F: FnOnce(T::Id) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Option<T>, FetchError>> + Send + 'static,
    {
        // Atomic registration via the entry API: probe + insert under
        // a single shard lock so two concurrent callers can't both
        // think they're the originator. Re-asserting the lookup under
        // the entry API closes the M1 read-then-mutate race window.
        let strong: StrongFetch<T> = match self.pending.entry(id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => match e.get().upgrade() {
                Some(strong) => strong,
                None => {
                    // Stale entry — the previous fetch was abandoned
                    // (case 2: all awaiters dropped). Replace it with
                    // a fresh fetch under the same shard lock.
                    let strong = build_fetch::<T, _, _>(id.clone(), fetcher);
                    e.insert(Arc::downgrade(&strong));
                    strong
                }
            },
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let strong = build_fetch::<T, _, _>(id.clone(), fetcher);
                e.insert(Arc::downgrade(&strong));
                strong
            }
        };

        // Clone the inner Shared so we can `.await` it. `Shared` is
        // itself ref-counted; this is cheap. The strong `Arc` we
        // hold (and `_strong_holder` below) keeps the underlying
        // fetch future alive until either (a) it completes or (b)
        // every awaiter drops — at which point the strong count of
        // the `Arc` reaches zero and the inner Shared is dropped,
        // dropping the fetch future. Real cancellation, no
        // background-task leak.
        let shared = (*strong).clone();
        let _strong_holder = strong;

        let out: SharedOutput<T> = shared.await;
        out.map_err(FetchError::from)
    }
}

/// Build a fresh fetch future for the given id + fetcher closure,
/// wrap it in [`Shared`], and return the `Arc<Shared<...>>` strong
/// handle. The caller stores `Arc::downgrade(...)` in the registry
/// and holds the strong handle for its own awaiter.
fn build_fetch<T, F, Fut>(id: T::Id, fetcher: F) -> StrongFetch<T>
where
    T: Cacheable,
    F: FnOnce(T::Id) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Option<T>, FetchError>> + Send + 'static,
{
    let type_name = std::any::type_name::<T>();
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
        let result = AssertUnwindSafe(async move { fetcher(id).await })
            .catch_unwind()
            .await;
        match result {
            Ok(Ok(opt)) => Ok(opt.map(Arc::new)),
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
    Arc::new(inner.shared())
}
