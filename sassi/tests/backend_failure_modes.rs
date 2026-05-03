#![cfg(all(feature = "serde", feature = "runtime-tokio"))]

use async_trait::async_trait;
use sassi::punnu::config::retry_delay_for_attempt;
use sassi::{
    BackendError, BackendFailureMode, BackendInvalidation, BackendKeyspace, CacheBackend,
    Cacheable, EventReason, Field, MemoryBackend, Punnu, PunnuConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct E {
    id: i64,
    label: String,
}

#[derive(Default)]
struct EFields {
    #[allow(dead_code)]
    id: Field<E, i64>,
}

impl Cacheable for E {
    type Id = i64;
    type Fields = EFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> EFields {
        EFields {
            id: Field::new("id", |e| &e.id),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct F {
    id: i64,
    label: String,
}

#[derive(Default)]
struct FFields {
    #[allow(dead_code)]
    id: Field<F, i64>,
}

impl Cacheable for F {
    type Id = i64;
    type Fields = FFields;

    fn id(&self) -> i64 {
        self.id
    }

    fn fields() -> FFields {
        FFields {
            id: Field::new("id", |f| &f.id),
        }
    }
}

#[tokio::test]
async fn memory_backend_round_trips_and_expires_wire_envelope() {
    let backend = MemoryBackend::default();
    let keyspace = keyspace::<E>(None);
    let value = E {
        id: 1,
        label: "one".into(),
    };

    backend
        .put(&keyspace, &value.id(), &value, Some(Duration::ZERO))
        .await
        .unwrap();

    assert_eq!(backend.get(&keyspace, &1_i64).await.unwrap(), None::<E>);

    backend
        .put(&keyspace, &value.id(), &value, None)
        .await
        .unwrap();
    assert_eq!(backend.get(&keyspace, &1_i64).await.unwrap(), Some(value));
}

#[test]
fn retry_delay_uses_capped_exponential_backoff() {
    assert_eq!(retry_delay_for_attempt(1), Duration::ZERO);
    assert_eq!(retry_delay_for_attempt(2), Duration::from_millis(25));
    assert_eq!(retry_delay_for_attempt(3), Duration::from_millis(50));
    assert_eq!(retry_delay_for_attempt(8), Duration::from_millis(1_000));
}

#[test]
#[should_panic(expected = "BackendFailureMode::Retry requires attempts >= 1")]
fn retry_zero_attempts_is_rejected_at_build() {
    let _: Punnu<E> = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 0 },
            ..Default::default()
        })
        .build();
}

#[tokio::test]
async fn error_mode_backend_insert_failure_does_not_mutate_l1() {
    let backend = FailingPutBackend::default();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Error,
            ..Default::default()
        })
        .backend(backend)
        .build();

    let err = punnu
        .insert(E {
            id: 7,
            label: "seven".into(),
        })
        .await
        .unwrap_err();

    assert!(matches!(err, sassi::InsertError::BackendFailed(_)));
    assert!(punnu.get(&7).is_none());
}

#[tokio::test]
async fn retry_mode_exhaustion_keeps_l1_success_after_total_attempts() {
    let backend = FailingPutBackend::default();
    let attempts = backend.put_attempts.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            backend_failure_mode: BackendFailureMode::Retry { attempts: 3 },
            ..Default::default()
        })
        .backend(backend)
        .build();

    let inserted = punnu
        .insert(E {
            id: 8,
            label: "eight".into(),
        })
        .await
        .unwrap();

    assert_eq!(inserted.id, 8);
    assert!(punnu.get(&8).is_some());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn get_async_rejects_backend_identity_mismatch_without_inserting() {
    let punnu = Punnu::<E>::builder().backend(WrongIdBackend).build();

    let err = punnu.get_async(&1).await.unwrap_err();

    assert!(matches!(err, BackendError::Serialization(_)));
    assert!(punnu.get(&1).is_none());
    assert!(punnu.get(&999).is_none());
}

#[tokio::test]
async fn backend_invalidation_stream_removes_l1_and_emits_backend_reason() {
    let (backend, tx) = StreamingBackend::new();
    let punnu = Punnu::<E>::builder().backend(backend).build();
    let mut events = punnu.events();

    punnu
        .insert(E {
            id: 1,
            label: "one".into(),
        })
        .await
        .unwrap();
    punnu
        .insert(E {
            id: 2,
            label: "two".into(),
        })
        .await
        .unwrap();
    drain_ready_events(&mut events);

    tx.unbounded_send(Ok(BackendInvalidation::Id(1))).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        event,
        sassi::PunnuEvent::Invalidate {
            id: 1,
            reason: EventReason::BackendInvalidation { .. }
        }
    ));
    assert!(punnu.get(&1).is_none());
    assert!(punnu.get(&2).is_some());

    tx.unbounded_send(Ok(BackendInvalidation::All)).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        event,
        sassi::PunnuEvent::Invalidate {
            id: 2,
            reason: EventReason::BackendInvalidation { .. }
        }
    ));
    assert!(punnu.get(&2).is_none());
}

#[tokio::test]
async fn punnu_backend_keyspace_uses_config_namespace_and_type_name() {
    let backend = RecordingBackend::default();
    let seen = backend.seen.clone();
    let punnu = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("tenant-a".into()),
            ..Default::default()
        })
        .backend(backend)
        .build();

    punnu
        .insert(E {
            id: 3,
            label: "three".into(),
        })
        .await
        .unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].namespace.as_deref(), Some("tenant-a"));
    assert_eq!(seen[0].type_name, std::any::type_name::<E>());
}

#[tokio::test]
async fn backend_invalidation_all_is_namespace_scoped_per_punnu() {
    let (backend_a, tx_a) = StreamingBackend::new();
    let (backend_b, _tx_b) = StreamingBackend::new();
    let punnu_a = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("env-a".into()),
            ..Default::default()
        })
        .backend(backend_a)
        .build();
    let punnu_b = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("env-b".into()),
            ..Default::default()
        })
        .backend(backend_b)
        .build();

    punnu_a
        .insert(E {
            id: 1,
            label: "a".into(),
        })
        .await
        .unwrap();
    punnu_b
        .insert(E {
            id: 1,
            label: "b".into(),
        })
        .await
        .unwrap();

    tx_a.unbounded_send(Ok(BackendInvalidation::All)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while punnu_a.get(&1).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(punnu_a.get(&1).is_none());
    assert_eq!(punnu_b.get(&1).unwrap().label, "b");
}

#[tokio::test]
async fn backend_invalidation_all_is_type_scoped_per_punnu() {
    let (backend_e, tx_e) = StreamingBackend::new();
    let (backend_f, _tx_f) = StreamingBackendForF::new();
    let punnu_e = Punnu::<E>::builder()
        .config(PunnuConfig {
            namespace: Some("shared-env".into()),
            ..Default::default()
        })
        .backend(backend_e)
        .build();
    let punnu_f = Punnu::<F>::builder()
        .config(PunnuConfig {
            namespace: Some("shared-env".into()),
            ..Default::default()
        })
        .backend(backend_f)
        .build();

    punnu_e
        .insert(E {
            id: 1,
            label: "e".into(),
        })
        .await
        .unwrap();
    punnu_f
        .insert(F {
            id: 1,
            label: "f".into(),
        })
        .await
        .unwrap();

    tx_e.unbounded_send(Ok(BackendInvalidation::All)).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while punnu_e.get(&1).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(punnu_e.get(&1).is_none());
    assert_eq!(punnu_f.get(&1).unwrap().label, "f");
}

fn keyspace<T: Cacheable>(namespace: Option<&str>) -> BackendKeyspace {
    BackendKeyspace {
        namespace: namespace.map(Arc::from),
        type_name: std::any::type_name::<T>(),
    }
}

fn drain_ready_events(rx: &mut tokio::sync::broadcast::Receiver<sassi::PunnuEvent<E>>) {
    while rx.try_recv().is_ok() {}
}

#[derive(Default)]
struct FailingPutBackend {
    put_attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl CacheBackend<E> for FailingPutBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        Err(BackendError::Network("down".into()))
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

struct WrongIdBackend;

#[async_trait]
impl CacheBackend<E> for WrongIdBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(Some(E {
            id: 999,
            label: "wrong".into(),
        }))
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

type InvalidationRx =
    futures::channel::mpsc::UnboundedReceiver<Result<BackendInvalidation<i64>, BackendError>>;

struct StreamingBackend {
    rx: Mutex<Option<InvalidationRx>>,
}

impl StreamingBackend {
    fn new() -> (
        Self,
        futures::channel::mpsc::UnboundedSender<Result<BackendInvalidation<i64>, BackendError>>,
    ) {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (
            Self {
                rx: Mutex::new(Some(rx)),
            },
            tx,
        )
    }
}

#[derive(Default)]
struct RecordingBackend {
    seen: Arc<Mutex<Vec<BackendKeyspaceSnapshot>>>,
}

#[derive(Debug, Clone)]
struct BackendKeyspaceSnapshot {
    namespace: Option<String>,
    type_name: &'static str,
}

impl From<&BackendKeyspace> for BackendKeyspaceSnapshot {
    fn from(value: &BackendKeyspace) -> Self {
        Self {
            namespace: value.namespace.as_ref().map(ToString::to_string),
            type_name: value.type_name,
        }
    }
}

#[async_trait]
impl CacheBackend<E> for RecordingBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        self.seen.lock().unwrap().push(keyspace.into());
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }
}

type InvalidationRxF =
    futures::channel::mpsc::UnboundedReceiver<Result<BackendInvalidation<i64>, BackendError>>;

struct StreamingBackendForF {
    rx: Mutex<Option<InvalidationRxF>>,
}

impl StreamingBackendForF {
    fn new() -> (
        Self,
        futures::channel::mpsc::UnboundedSender<Result<BackendInvalidation<i64>, BackendError>>,
    ) {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (
            Self {
                rx: Mutex::new(Some(rx)),
            },
            tx,
        )
    }
}

#[async_trait]
impl CacheBackend<F> for StreamingBackendForF {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<F>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &F,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(
        &self,
        _keyspace: BackendKeyspace,
    ) -> sassi::BackendInvalidationStream<i64> {
        Box::pin(
            self.rx
                .lock()
                .unwrap()
                .take()
                .expect("stream should be subscribed once"),
        )
    }
}

#[async_trait]
impl CacheBackend<E> for StreamingBackend {
    async fn get(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<Option<E>, BackendError> {
        Ok(None)
    }

    async fn put(
        &self,
        _keyspace: &BackendKeyspace,
        _id: &i64,
        _value: &E,
        _ttl: Option<Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate(&self, _keyspace: &BackendKeyspace, _id: &i64) -> Result<(), BackendError> {
        Ok(())
    }

    async fn invalidate_all(&self, _keyspace: &BackendKeyspace) -> Result<(), BackendError> {
        Ok(())
    }

    fn invalidation_stream(
        &self,
        _keyspace: BackendKeyspace,
    ) -> sassi::BackendInvalidationStream<i64> {
        Box::pin(
            self.rx
                .lock()
                .unwrap()
                .take()
                .expect("stream should be subscribed once"),
        )
    }
}
