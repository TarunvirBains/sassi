//! [`PunnuScope<T>`] - owned query handle over a typed pool.
//!
//! A scope snapshots live `Arc<T>` entries from a [`Punnu`](crate::punnu::Punnu)
//! and then applies a lazy [`MemQ`](crate::predicate::MemQ) pipeline.
//! The handle owns a cloned pool handle instead of borrowing from the
//! pool, so callers can build scopes freely and move them across
//! function boundaries without lifetime plumbing.

use crate::cacheable::Cacheable;
use crate::predicate::{BasicPredicate, MemQ};
use crate::punnu::Punnu;
use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::Arc;

/// Owned in-memory query handle for a [`Punnu<T>`](crate::punnu::Punnu).
///
/// `PunnuScope` accumulates [`MemQ<T>`](crate::predicate::MemQ)
/// operations without touching the pool. Terminal methods
/// ([`collect`](Self::collect), [`iter`](Self::iter),
/// [`first`](Self::first), [`count`](Self::count), and
/// [`exists`](Self::exists)) snapshot the pool and execute the
/// accumulated operations in order.
pub struct PunnuScope<T: Cacheable> {
    pool: Arc<Punnu<T>>,
    ops: Vec<MemQ<T>>,
}

impl<T: Cacheable> PunnuScope<T> {
    /// Construct a scope from an owned pool handle and an initial
    /// operation list.
    ///
    /// This is public for orchestrator-style callers that already
    /// store pools in `Arc`s. Most direct pool users call
    /// [`Punnu::scope`](crate::punnu::Punnu::scope) instead.
    pub fn new(pool: Arc<Punnu<T>>, ops: impl Into<Vec<MemQ<T>>>) -> Self {
        Self {
            pool,
            ops: ops.into(),
        }
    }

    /// Append a ready-made operation to the scope.
    ///
    /// This is the escape hatch for callers that build `MemQ`
    /// pipelines separately and then attach them to a pool.
    pub fn then(mut self, op: MemQ<T>) -> Self {
        self.ops.push(op);
        self
    }

    /// Append a closure-DSL filter that returns a `MemQ` node.
    ///
    /// The field companion is built through `T::fields()` so field
    /// accessors are wired to real extractors.
    pub fn filter<F>(self, build: F) -> Self
    where
        F: FnOnce(T::Fields) -> MemQ<T>,
    {
        self.then(build(T::fields()))
    }

    /// Append a shared field-predicate filter.
    ///
    /// This keeps pure field predicates on the common
    /// [`BasicPredicate`](crate::predicate::BasicPredicate) path while
    /// still executing in-memory through `MemQ`.
    pub fn filter_basic<F>(self, build: F) -> Self
    where
        F: FnOnce(T::Fields) -> BasicPredicate<T>,
    {
        self.then(MemQ::filter_basic(build(T::fields())))
    }

    /// Append a Rust closure filter.
    ///
    /// The closure is evaluated in-memory only and cannot be lowered
    /// to SQL or serialized.
    pub fn filter_closure<F>(self, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.then(MemQ::filter(predicate))
    }

    /// Append an `Arc<T>` mapper.
    pub fn map_arc<F>(self, map: F) -> Self
    where
        F: Fn(Arc<T>) -> Arc<T> + Send + Sync + 'static,
    {
        self.then(MemQ::map_arc(map))
    }

    /// Append an `Arc<T>` flat mapper.
    pub fn flat_map_arc<F>(self, flat_map: F) -> Self
    where
        F: Fn(Arc<T>) -> Vec<Arc<T>> + Send + Sync + 'static,
    {
        self.then(MemQ::flat_map_arc(flat_map))
    }

    /// Keep at most the first `count` entries after earlier
    /// operations run.
    pub fn take(self, count: usize) -> Self {
        self.then(MemQ::take(count))
    }

    /// Drop the first `count` entries after earlier operations run.
    pub fn skip(self, count: usize) -> Self {
        self.then(MemQ::skip(count))
    }

    /// Append pre-shared entries to the scope.
    pub fn chain<I>(self, values: I) -> Self
    where
        I: IntoIterator<Item = Arc<T>>,
    {
        self.then(MemQ::chain(values))
    }

    /// Sort with a comparator over shared entries.
    pub fn sort<F>(self, compare: F) -> Self
    where
        F: Fn(&Arc<T>, &Arc<T>) -> Ordering + Send + Sync + 'static,
    {
        self.then(MemQ::sort(compare))
    }

    /// Sort by a derived key.
    pub fn sort_by_key<K, F>(self, key: F) -> Self
    where
        K: Ord,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        self.then(MemQ::sort_by_key(key))
    }

    /// Keep the first entry for each `T::id()`.
    pub fn unique(self) -> Self {
        self.then(MemQ::unique())
    }

    /// Keep the first entry for each caller-supplied key.
    pub fn unique_by<K, F>(self, key: F) -> Self
    where
        K: Eq + Hash + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        self.then(MemQ::unique_by(key))
    }

    /// Group by a caller-supplied key, then flatten buckets back into
    /// the sequence.
    pub fn group_by<K, F>(self, key: F) -> Self
    where
        K: Eq + Hash + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        self.then(MemQ::group_by(key))
    }

    /// Move matching entries before non-matching entries.
    pub fn partition<F>(self, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.then(MemQ::partition(predicate))
    }

    /// Fold the whole sequence to zero or one entry.
    pub fn fold<F>(self, fold: F) -> Self
    where
        F: Fn(Vec<Arc<T>>) -> Option<Arc<T>> + Send + Sync + 'static,
    {
        self.then(MemQ::fold(fold))
    }

    /// Execute the accumulated operations and return shared entries.
    ///
    /// The pool is snapshotted first, skipping entries whose TTL has
    /// already elapsed. The scope then applies every `MemQ` operation
    /// in the order it was added.
    pub fn collect(self) -> Vec<Arc<T>> {
        MemQ::apply_all(&self.ops, self.pool.snapshot_unexpired())
    }

    /// Execute the scope and return a consuming iterator.
    pub fn iter(self) -> std::vec::IntoIter<Arc<T>> {
        self.collect().into_iter()
    }

    /// Return the first entry from the executed scope.
    pub fn first(self) -> Option<Arc<T>> {
        self.take(1).collect().into_iter().next()
    }

    /// Count entries produced by the executed scope.
    pub fn count(self) -> usize {
        self.collect().len()
    }

    /// Return `true` when the executed scope yields at least one
    /// entry.
    pub fn exists(self) -> bool {
        self.first().is_some()
    }
}

impl<T> PunnuScope<T>
where
    T: Cacheable + Clone,
{
    /// Append a mapper that returns owned replacement values.
    pub fn map<F>(self, map: F) -> Self
    where
        F: Fn(&T) -> T + Send + Sync + 'static,
    {
        self.then(MemQ::map(map))
    }

    /// Append a flat mapper that returns owned replacement values.
    pub fn flat_map<F>(self, flat_map: F) -> Self
    where
        F: Fn(&T) -> Vec<T> + Send + Sync + 'static,
    {
        self.then(MemQ::flat_map(flat_map))
    }

    /// Append owned values to the scope.
    pub fn chain_values<I>(self, values: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        self.then(MemQ::chain_values(values))
    }
}
