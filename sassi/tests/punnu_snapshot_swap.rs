//! Snapshot-swap L1 concurrency contract.

use sassi::{Cacheable, Field, Punnu};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock, mpsc};
use std::time::Duration;

static BLOCK_VALUE: AtomicI64 = AtomicI64::new(i64::MIN);
static BLOCK_ACTIVE: AtomicBool = AtomicBool::new(false);
static HASH_BLOCKER: OnceLock<HashBlocker> = OnceLock::new();

struct HashBlocker {
    entered: (Mutex<bool>, Condvar),
    released: (Mutex<bool>, Condvar),
}

impl HashBlocker {
    fn global() -> &'static Self {
        HASH_BLOCKER.get_or_init(|| Self {
            entered: (Mutex::new(false), Condvar::new()),
            released: (Mutex::new(false), Condvar::new()),
        })
    }

    fn arm(value: i64) {
        let blocker = Self::global();
        *blocker.entered.0.lock().unwrap() = false;
        *blocker.released.0.lock().unwrap() = false;
        BLOCK_VALUE.store(value, Ordering::SeqCst);
        BLOCK_ACTIVE.store(true, Ordering::SeqCst);
    }

    fn wait_until_entered() {
        let blocker = Self::global();
        let mut entered = blocker.entered.0.lock().unwrap();
        while !*entered {
            entered = blocker.entered.1.wait(entered).unwrap();
        }
    }

    fn release() {
        let blocker = Self::global();
        BLOCK_ACTIVE.store(false, Ordering::SeqCst);
        let mut released = blocker.released.0.lock().unwrap();
        *released = true;
        blocker.released.1.notify_all();
    }

    fn block_here() {
        let blocker = Self::global();
        {
            let mut entered = blocker.entered.0.lock().unwrap();
            *entered = true;
            blocker.entered.1.notify_all();
        }

        let mut released = blocker.released.0.lock().unwrap();
        while !*released {
            released = blocker.released.1.wait(released).unwrap();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BlockingId(i64);

impl Hash for BlockingId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if BLOCK_ACTIVE.load(Ordering::SeqCst) && self.0 == BLOCK_VALUE.load(Ordering::SeqCst) {
            HashBlocker::block_here();
        }
        self.0.hash(state);
    }
}

#[derive(Debug, Clone)]
struct Item {
    id: BlockingId,
    label: &'static str,
}

#[derive(Default)]
struct ItemFields {
    #[allow(dead_code)]
    label: Field<Item, &'static str>,
}

impl Cacheable for Item {
    type Id = BlockingId;
    type Fields = ItemFields;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn fields() -> Self::Fields {
        ItemFields {
            label: Field::new("label", |item| &item.label),
        }
    }
}

#[test]
fn readers_observe_old_or_new_state_without_partial_publish() {
    let punnu = Punnu::<Item>::builder().build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime
        .block_on(punnu.insert(Item {
            id: BlockingId(1),
            label: "old",
        }))
        .unwrap();

    HashBlocker::arm(2);
    let writer_punnu = punnu.clone();
    let writer = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(writer_punnu.insert(Item {
                id: BlockingId(2),
                label: "new",
            }))
            .unwrap();
    });

    HashBlocker::wait_until_entered();

    let (tx, rx) = mpsc::channel();
    let reader_punnu = punnu.clone();
    let reader = std::thread::spawn(move || {
        let observed = reader_punnu.get(&BlockingId(1)).map(|item| item.label);
        tx.send(observed).unwrap();
    });

    let observed = rx.recv_timeout(Duration::from_millis(100));
    HashBlocker::release();
    writer.join().unwrap();
    reader.join().unwrap();

    let observed = observed.expect("get must not wait for an unrelated in-flight L1 write");
    assert_eq!(
        observed,
        Some("old"),
        "reader should observe the old committed snapshot while the new snapshot is prepared"
    );

    let new = punnu.get(&BlockingId(2)).expect("new value should publish");
    assert_eq!(new.label, "new");
    assert_eq!(punnu.len(), 2);

    let old = punnu.get(&BlockingId(1)).expect("old value should remain");
    assert_eq!(old.label, "old");
}
