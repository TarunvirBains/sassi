//! [`MemQ<T>`] - the in-memory extension algebra.
//!
//! `MemQ` is deliberately lazy. Each value describes one operation
//! over a vector of cached `Arc<T>` entries; operations do no work
//! until a caller applies them directly with [`MemQ::apply_all`] or
//! indirectly through [`crate::punnu::PunnuScope::collect`].
//!
//! The algebra stays Rust-only. It can hold closures, comparators,
//! and grouping keys that cannot be lowered to SQL or a wire format.
//! Pure field predicates still enter the pipeline through
//! [`MemQ::filter_basic`], which evaluates the shared
//! [`BasicPredicate`] tree.

use crate::cacheable::Cacheable;
use crate::predicate::{BasicPredicate, IntoBasicPredicate};
use std::any::{Any, TypeId};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

type ArcValue<T> = Arc<T>;
type PredicateFn<T> = dyn Fn(&T) -> bool + Send + Sync;
type MapFn<T> = dyn Fn(ArcValue<T>) -> ArcValue<T> + Send + Sync;
type FlatMapFn<T> = dyn Fn(ArcValue<T>) -> Vec<ArcValue<T>> + Send + Sync;
type CompareFn<T> = dyn Fn(&ArcValue<T>, &ArcValue<T>) -> Ordering + Send + Sync;
type KeyFn<T> = dyn Fn(&T) -> ErasedKey + Send + Sync;
type FoldFn<T> = dyn Fn(Vec<ArcValue<T>>) -> Option<ArcValue<T>> + Send + Sync;

/// Lazy in-memory query operation over cached `Arc<T>` entries.
///
/// Variants are public so the algebra is inspectable, but their
/// payload types have private fields and the enum is
/// `#[non_exhaustive]`. Construct operations with the associated
/// builder methods (`filter`, `map`, `sort_by_key`, and friends)
/// rather than assembling payloads directly.
#[non_exhaustive]
#[derive(Clone)]
pub enum MemQ<T: Cacheable> {
    /// Keep entries that satisfy a field predicate or Rust closure.
    Filter(Filter<T>),
    /// Replace each entry with another `Arc<T>`.
    Map(Map<T>),
    /// Replace each entry with zero or more `Arc<T>` entries.
    FlatMap(FlatMap<T>),
    /// Keep at most the first `n` entries.
    Take(Take),
    /// Drop the first `n` entries.
    Skip(Skip),
    /// Append additional entries to the current sequence.
    Chain(Chain<T>),
    /// Sort the current sequence.
    Sort(Sort<T>),
    /// Keep the first entry for each key.
    Unique(Unique<T>),
    /// Bucket entries by key, then flatten buckets in first-seen key
    /// order.
    GroupBy(GroupBy<T>),
    /// Move matching entries before non-matching entries while
    /// preserving order inside each side.
    Partition(Partition<T>),
    /// Reduce the whole sequence to zero or one entry.
    Fold(Fold<T>),
}

/// Payload for [`MemQ::Filter`].
///
/// Constructed by [`MemQ::filter`] or [`MemQ::filter_basic`]. The
/// fields stay private so downstream crates cannot forge filter nodes
/// outside the builder surface.
#[derive(Clone)]
pub struct Filter<T: Cacheable> {
    kind: FilterKind<T>,
}

#[derive(Clone)]
enum FilterKind<T: Cacheable> {
    Basic(BasicPredicate<T>),
    Closure(Arc<PredicateFn<T>>),
}

impl<T: Cacheable> Filter<T> {
    fn evaluate(&self, value: &T) -> bool {
        match &self.kind {
            FilterKind::Basic(predicate) => predicate.evaluate(value),
            FilterKind::Closure(predicate) => {
                catch_unwind(AssertUnwindSafe(|| predicate(value))).unwrap_or(false)
            }
        }
    }
}

/// Payload for [`MemQ::Map`].
///
/// The mapper consumes and returns `Arc<T>` so it can either preserve
/// identity handles or mint replacement values without borrowing from
/// the input sequence.
#[derive(Clone)]
pub struct Map<T: Cacheable> {
    map: Arc<MapFn<T>>,
}

/// Payload for [`MemQ::FlatMap`].
///
/// The mapper can drop an entry by returning an empty vector, keep it
/// by returning one value, or expand it by returning many values.
#[derive(Clone)]
pub struct FlatMap<T: Cacheable> {
    flat_map: Arc<FlatMapFn<T>>,
}

/// Payload for [`MemQ::Take`].
///
/// Keeps the first `count` entries after all earlier operations have
/// run.
#[derive(Clone)]
pub struct Take {
    count: usize,
}

/// Payload for [`MemQ::Skip`].
///
/// Drops the first `count` entries after all earlier operations have
/// run.
#[derive(Clone)]
pub struct Skip {
    count: usize,
}

/// Payload for [`MemQ::Chain`].
///
/// Appended entries are already `Arc<T>` so the pipeline does not
/// clone cached values when it reaches this operation.
#[derive(Clone)]
pub struct Chain<T: Cacheable> {
    values: Vec<Arc<T>>,
}

/// Payload for [`MemQ::Sort`].
///
/// Holds a comparator over `Arc<T>` entries. Use
/// [`MemQ::sort_by_key`] when a simple ordered key is enough.
#[derive(Clone)]
pub struct Sort<T: Cacheable> {
    compare: Arc<CompareFn<T>>,
}

/// Payload for [`MemQ::Unique`].
///
/// Keeps first-seen entries by an erased key. [`MemQ::unique`] uses
/// `T::id()`; [`MemQ::unique_by`] lets callers choose a typed key.
#[derive(Clone)]
pub struct Unique<T: Cacheable> {
    key: Arc<KeyFn<T>>,
}

/// Payload for [`MemQ::GroupBy`].
///
/// Groups by an erased key and flattens the buckets back into a
/// sequence. This preserves `collect() -> Vec<Arc<T>>` while still
/// letting callers express grouping as a lazy algebra node.
#[derive(Clone)]
pub struct GroupBy<T: Cacheable> {
    key: Arc<KeyFn<T>>,
}

/// Payload for [`MemQ::Partition`].
///
/// The predicate is evaluated once per entry. Matching entries keep
/// their relative order and are returned before non-matching entries.
#[derive(Clone)]
pub struct Partition<T: Cacheable> {
    predicate: Arc<PredicateFn<T>>,
}

/// Payload for [`MemQ::Fold`].
///
/// The fold sees the full sequence after earlier operations and
/// returns either a single replacement entry or `None` for an empty
/// result.
#[derive(Clone)]
pub struct Fold<T: Cacheable> {
    fold: Arc<FoldFn<T>>,
}

impl<T: Cacheable> MemQ<T> {
    /// Construct a closure-backed filter node.
    ///
    /// Panics inside the closure are caught at evaluation time and
    /// treated as `false` for that entry, matching the predicate
    /// contract that query evaluation should stay total.
    pub fn filter<F>(predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self::Filter(Filter {
            kind: FilterKind::Closure(Arc::new(predicate)),
        })
    }

    /// Construct a filter node from the shared field-predicate
    /// algebra.
    ///
    /// This is the bridge from SQL-projectable
    /// [`BasicPredicate`] into the
    /// Rust-only `MemQ` pipeline.
    pub fn filter_basic<P>(predicate: P) -> Self
    where
        P: IntoBasicPredicate<T>,
    {
        Self::Filter(Filter {
            kind: FilterKind::Basic(predicate.into_basic_predicate()),
        })
    }

    /// Construct an `Arc<T>` mapper.
    ///
    /// Use this when the mapper wants to preserve an existing `Arc`,
    /// share a pre-built value, or avoid cloning `T`.
    pub fn map_arc<F>(map: F) -> Self
    where
        F: Fn(Arc<T>) -> Arc<T> + Send + Sync + 'static,
    {
        Self::Map(Map { map: Arc::new(map) })
    }

    /// Construct a flat mapper over `Arc<T>` entries.
    ///
    /// The closure returns the replacement entries for one input
    /// entry.
    pub fn flat_map_arc<F>(flat_map: F) -> Self
    where
        F: Fn(Arc<T>) -> Vec<Arc<T>> + Send + Sync + 'static,
    {
        Self::FlatMap(FlatMap {
            flat_map: Arc::new(flat_map),
        })
    }

    /// Keep at most the first `count` entries.
    pub fn take(count: usize) -> Self {
        Self::Take(Take { count })
    }

    /// Drop the first `count` entries.
    pub fn skip(count: usize) -> Self {
        Self::Skip(Skip { count })
    }

    /// Append pre-shared entries to the sequence.
    pub fn chain<I>(values: I) -> Self
    where
        I: IntoIterator<Item = Arc<T>>,
    {
        Self::Chain(Chain {
            values: values.into_iter().collect(),
        })
    }

    /// Sort with a custom comparator over shared entries.
    pub fn sort<F>(compare: F) -> Self
    where
        F: Fn(&Arc<T>, &Arc<T>) -> Ordering + Send + Sync + 'static,
    {
        Self::Sort(Sort {
            compare: Arc::new(compare),
        })
    }

    /// Keep the first entry for each `T::id()`.
    pub fn unique() -> Self {
        Self::unique_by(|value: &T| value.id())
    }

    /// Keep the first entry for each caller-supplied key.
    pub fn unique_by<K, F>(key: F) -> Self
    where
        K: Eq + Hash + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        Self::Unique(Unique {
            key: Arc::new(move |value| ErasedKey::new(key(value))),
        })
    }

    /// Group entries by a caller-supplied key, then flatten buckets
    /// in first-seen key order.
    pub fn group_by<K, F>(key: F) -> Self
    where
        K: Eq + Hash + Send + Sync + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        Self::GroupBy(GroupBy {
            key: Arc::new(move |value| ErasedKey::new(key(value))),
        })
    }

    /// Partition entries into matching and non-matching sides.
    pub fn partition<F>(predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self::Partition(Partition {
            predicate: Arc::new(predicate),
        })
    }

    /// Fold the whole sequence to zero or one entry.
    pub fn fold<F>(fold: F) -> Self
    where
        F: Fn(Vec<Arc<T>>) -> Option<Arc<T>> + Send + Sync + 'static,
    {
        Self::Fold(Fold {
            fold: Arc::new(fold),
        })
    }

    /// Apply this operation to an already-materialized sequence.
    ///
    /// Most callers should use [`PunnuScope`](crate::punnu::PunnuScope)
    /// so materialization happens from the pool. This method exists
    /// for tests, diagnostics, and consumers that already hold a
    /// sequence of `Arc<T>`.
    pub fn apply(&self, values: Vec<Arc<T>>) -> Vec<Arc<T>> {
        match self {
            Self::Filter(op) => values
                .into_iter()
                .filter(|value| op.evaluate(value))
                .collect(),
            Self::Map(op) => values.into_iter().map(|value| (op.map)(value)).collect(),
            Self::FlatMap(op) => values
                .into_iter()
                .flat_map(|value| (op.flat_map)(value))
                .collect(),
            Self::Take(op) => values.into_iter().take(op.count).collect(),
            Self::Skip(op) => values.into_iter().skip(op.count).collect(),
            Self::Chain(op) => values
                .into_iter()
                .chain(op.values.iter().cloned())
                .collect(),
            Self::Sort(op) => {
                let mut values = values;
                values.sort_by(|left, right| (op.compare)(left, right));
                values
            }
            Self::Unique(op) => unique_by(values, &op.key),
            Self::GroupBy(op) => group_by(values, &op.key),
            Self::Partition(op) => partition(values, &op.predicate),
            Self::Fold(op) => (op.fold)(values).into_iter().collect(),
        }
    }

    /// Apply a list of operations in order.
    ///
    /// The operations stay lazy until this call. Each operation
    /// consumes the previous operation's vector and returns the next
    /// vector, so no intermediate borrowing from the source pool is
    /// required.
    pub fn apply_all(ops: &[Self], values: Vec<Arc<T>>) -> Vec<Arc<T>> {
        ops.iter().fold(values, |values, op| op.apply(values))
    }
}

impl<T> MemQ<T>
where
    T: Cacheable + Clone,
{
    /// Construct a mapper that clones `T` into a replacement value.
    ///
    /// This is the ergonomic wrapper around [`MemQ::map_arc`] for
    /// callers that want to transform values rather than handles.
    pub fn map<F>(map: F) -> Self
    where
        F: Fn(&T) -> T + Send + Sync + 'static,
    {
        Self::map_arc(move |value| Arc::new(map(&value)))
    }

    /// Construct a flat mapper that returns owned replacement values.
    ///
    /// Each returned value is wrapped in an `Arc<T>` before the next
    /// operation runs.
    pub fn flat_map<F>(flat_map: F) -> Self
    where
        F: Fn(&T) -> Vec<T> + Send + Sync + 'static,
    {
        Self::flat_map_arc(move |value| flat_map(&value).into_iter().map(Arc::new).collect())
    }

    /// Append owned values to the sequence.
    pub fn chain_values<I>(values: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self::chain(values.into_iter().map(Arc::new))
    }
}

impl<T: Cacheable> From<BasicPredicate<T>> for MemQ<T> {
    fn from(predicate: BasicPredicate<T>) -> Self {
        Self::filter_basic(predicate)
    }
}

impl<T: Cacheable> MemQ<T> {
    /// Sort by a derived key.
    ///
    /// Keys are computed during comparison. If key extraction is
    /// expensive, precompute the key in a preceding [`MemQ::map`]
    /// operation or use [`MemQ::sort`] with a custom comparator.
    pub fn sort_by_key<K, F>(key: F) -> Self
    where
        K: Ord,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        Self::sort(move |left, right| key(left).cmp(&key(right)))
    }
}

fn unique_by<T: Cacheable>(values: Vec<Arc<T>>, key: &Arc<KeyFn<T>>) -> Vec<Arc<T>> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(key(value)))
        .collect()
}

fn group_by<T: Cacheable>(values: Vec<Arc<T>>, key: &Arc<KeyFn<T>>) -> Vec<Arc<T>> {
    let mut index_by_key: HashMap<ErasedKey, usize> = HashMap::new();
    let mut groups: Vec<Vec<Arc<T>>> = Vec::new();

    for value in values {
        let key = key(&value);
        if let Some(index) = index_by_key.get(&key).copied() {
            groups[index].push(value);
        } else {
            let index = groups.len();
            index_by_key.insert(key, index);
            groups.push(vec![value]);
        }
    }

    groups.into_iter().flatten().collect()
}

fn partition<T: Cacheable>(values: Vec<Arc<T>>, predicate: &Arc<PredicateFn<T>>) -> Vec<Arc<T>> {
    let mut matches = Vec::new();
    let mut misses = Vec::new();

    for value in values {
        if catch_unwind(AssertUnwindSafe(|| predicate(&value))).unwrap_or(false) {
            matches.push(value);
        } else {
            misses.push(value);
        }
    }

    matches.extend(misses);
    matches
}

struct ErasedKey {
    type_id: TypeId,
    hash: u64,
    value: Box<dyn Any + Send + Sync>,
    eq: fn(&(dyn Any + Send + Sync), &(dyn Any + Send + Sync)) -> bool,
}

impl ErasedKey {
    fn new<K>(value: K) -> Self
    where
        K: Eq + Hash + Send + Sync + 'static,
    {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        Self {
            type_id: TypeId::of::<K>(),
            hash: hasher.finish(),
            value: Box::new(value),
            eq: erased_eq::<K>,
        }
    }
}

impl PartialEq for ErasedKey {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
            && self.hash == other.hash
            && (self.eq)(&*self.value, &*other.value)
    }
}

impl Eq for ErasedKey {}

impl Hash for ErasedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
        self.hash.hash(state);
    }
}

fn erased_eq<K>(left: &(dyn Any + Send + Sync), right: &(dyn Any + Send + Sync)) -> bool
where
    K: Eq + 'static,
{
    match (left.downcast_ref::<K>(), right.downcast_ref::<K>()) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}
