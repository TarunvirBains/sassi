use futures::StreamExt;
use sassi::{BackendInvalidation, BackendKeyspace, CacheBackend, Cacheable, Field};
use sassi_cache_redis::RedisBackend;
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

fn redis_client() -> Option<redis::Client> {
    std::env::var("REDIS_URL")
        .ok()
        .and_then(|url| redis::Client::open(url).ok())
}

fn namespace(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("sassi-test-{label}-{nanos}")
}

fn keyspace<T: Cacheable>(namespace: String) -> BackendKeyspace {
    BackendKeyspace {
        namespace: Some(Arc::from(namespace)),
        type_name: std::any::type_name::<T>(),
    }
}

struct FakePubSubServer {
    addr: SocketAddr,
    handle: thread::JoinHandle<io::Result<()>>,
}

struct ClosingPubSubServer {
    addr: SocketAddr,
    handle: thread::JoinHandle<io::Result<()>>,
}

impl ClosingPubSubServer {
    fn start(connections: usize) -> io::Result<(Self, mpsc::Receiver<std::time::Instant>)> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for _ in 0..connections {
                let (mut stream, _) = listener.accept()?;
                handle_closing_pubsub_connection(&mut stream, &tx)?;
            }
            Ok(())
        });

        Ok((Self { addr, handle }, rx))
    }

    fn url(&self) -> String {
        format!("redis://{}/", self.addr)
    }

    fn join(self) -> io::Result<()> {
        self.handle
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

impl FakePubSubServer {
    fn start(payloads: Vec<Vec<u8>>) -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        let handle = thread::spawn(move || {
            for payload in payloads {
                let (mut stream, _) = listener.accept()?;
                handle_fake_pubsub_connection(&mut stream, &payload)?;
            }
            Ok(())
        });

        Ok(Self { addr, handle })
    }

    fn url(&self) -> String {
        format!("redis://{}/", self.addr)
    }

    fn join(self) -> io::Result<()> {
        self.handle
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    }
}

fn handle_closing_pubsub_connection(
    stream: &mut TcpStream,
    subscribed_at: &mpsc::Sender<std::time::Instant>,
) -> io::Result<()> {
    loop {
        let command = read_resp_command(stream)?;
        let Some(name) = command.first() else {
            continue;
        };

        if name.eq_ignore_ascii_case("CLIENT") {
            stream.write_all(b"+OK\r\n")?;
            stream.flush()?;
            continue;
        }

        if name.eq_ignore_ascii_case("SUBSCRIBE") {
            let channel = command.get(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "SUBSCRIBE missing channel")
            })?;
            write_subscribe_ack(stream, channel)?;
            subscribed_at
                .send(std::time::Instant::now())
                .map_err(|err| io::Error::new(io::ErrorKind::BrokenPipe, err))?;
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(());
        }

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected Redis command: {command:?}"),
        ));
    }
}

fn handle_fake_pubsub_connection(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    loop {
        let command = read_resp_command(stream)?;
        let Some(name) = command.first() else {
            continue;
        };

        if name.eq_ignore_ascii_case("CLIENT") {
            stream.write_all(b"+OK\r\n")?;
            stream.flush()?;
            continue;
        }

        if name.eq_ignore_ascii_case("SUBSCRIBE") {
            let channel = command.get(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "SUBSCRIBE missing channel")
            })?;
            write_subscribe_ack(stream, channel)?;
            write_pubsub_message(stream, channel, payload)?;
            let _ = stream.shutdown(Shutdown::Both);
            return Ok(());
        }

        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected Redis command: {command:?}"),
        ));
    }
}

fn read_resp_command(stream: &mut TcpStream) -> io::Result<Vec<String>> {
    let line = read_resp_line(stream)?;
    let count = line
        .strip_prefix('*')
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected RESP array"))?
        .parse::<usize>()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;

    let mut command = Vec::with_capacity(count);
    for _ in 0..count {
        let line = read_resp_line(stream)?;
        let len = line
            .strip_prefix('$')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "expected bulk string"))?
            .parse::<usize>()
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let mut bytes = vec![0; len];
        stream.read_exact(&mut bytes)?;
        let terminator = read_resp_line(stream)?;
        if !terminator.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bulk string missing CRLF terminator",
            ));
        }
        command.push(
            String::from_utf8(bytes)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?,
        );
    }

    Ok(command)
}

fn read_resp_line(stream: &mut TcpStream) -> io::Result<String> {
    let mut bytes = Vec::new();
    let mut byte = [0];
    loop {
        stream.read_exact(&mut byte)?;
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n") {
            bytes.truncate(bytes.len() - 2);
            return String::from_utf8(bytes)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err));
        }
    }
}

fn write_subscribe_ack(stream: &mut TcpStream, channel: &str) -> io::Result<()> {
    stream.write_all(b"*3\r\n$9\r\nsubscribe\r\n")?;
    write_bulk_string(stream, channel.as_bytes())?;
    stream.write_all(b":1\r\n")?;
    stream.flush()
}

fn write_pubsub_message(stream: &mut TcpStream, channel: &str, payload: &[u8]) -> io::Result<()> {
    stream.write_all(b"*3\r\n$7\r\nmessage\r\n")?;
    write_bulk_string(stream, channel.as_bytes())?;
    write_bulk_string(stream, payload)?;
    stream.flush()
}

fn write_bulk_string(stream: &mut TcpStream, bytes: &[u8]) -> io::Result<()> {
    write!(stream, "${}\r\n", bytes.len())?;
    stream.write_all(bytes)?;
    stream.write_all(b"\r\n")
}

#[tokio::test]
async fn redis_invalidation_stream_reconnects_after_pubsub_stream_ends() {
    let server = FakePubSubServer::start(vec![
        serde_json::to_vec(&BackendInvalidation::Id(11)).unwrap(),
        serde_json::to_vec(&BackendInvalidation::Id(12)).unwrap(),
    ])
    .unwrap();
    let client = redis::Client::open(server.url()).unwrap();
    let backend = RedisBackend::<E>::new(client);
    let keyspace = keyspace::<E>(namespace("stream-reconnect"));
    let mut stream = backend.invalidation_stream(keyspace);

    let message = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message, BackendInvalidation::Id(11));

    let message = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message, BackendInvalidation::Id(12));

    server.join().unwrap();
}

#[tokio::test]
async fn redis_invalidation_stream_backs_off_when_subscribe_succeeds_then_stream_ends() {
    let (server, subscribed_at) = ClosingPubSubServer::start(3).unwrap();
    let client = redis::Client::open(server.url()).unwrap();
    let backend = RedisBackend::<E>::new(client);
    let keyspace = keyspace::<E>(namespace("stream-backoff"));
    let mut stream = backend.invalidation_stream(keyspace);

    let poller = tokio::spawn(async move {
        let _ = stream.next().await;
    });

    let times = tokio::task::spawn_blocking(move || {
        (0..3)
            .map(|_| subscribed_at.recv_timeout(Duration::from_secs(2)).unwrap())
            .collect::<Vec<_>>()
    })
    .await
    .unwrap();

    poller.abort();
    server.join().unwrap();

    let first_gap = times[1].duration_since(times[0]);
    let second_gap = times[2].duration_since(times[1]);
    assert!(
        first_gap >= Duration::from_millis(5),
        "first reconnect gap was too short: {first_gap:?}"
    );
    assert!(
        second_gap >= first_gap + Duration::from_millis(5),
        "reconnect delay should increase after repeated stream endings; \
         first gap {first_gap:?}, second gap {second_gap:?}"
    );
}

#[tokio::test]
async fn redis_invalidation_stream_receives_id_and_all_messages() {
    let Some(client) = redis_client() else {
        eprintln!("skipping redis test because REDIS_URL is not set");
        return;
    };
    let writer = RedisBackend::<E>::new(client.clone());
    let subscriber = RedisBackend::<E>::new(client);
    let keyspace = keyspace::<E>(namespace("stream"));
    let mut stream = subscriber.invalidation_stream(keyspace.clone());

    writer
        .put(
            &keyspace,
            &7,
            &E {
                id: 7,
                label: "seven".into(),
            },
            None,
        )
        .await
        .unwrap();

    let publish = {
        let writer = writer.clone();
        let keyspace = keyspace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            writer.invalidate(&keyspace, &7).await.unwrap();
        })
    };

    let message = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    publish.await.unwrap();
    assert_eq!(message, BackendInvalidation::Id(7));

    let publish = {
        let writer = writer.clone();
        let keyspace = keyspace.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            writer.invalidate_all(&keyspace).await.unwrap();
        })
    };

    let message = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    publish.await.unwrap();
    assert_eq!(message, BackendInvalidation::All);
}

#[tokio::test]
async fn redis_invalidate_all_is_namespace_scoped() {
    let Some(client) = redis_client() else {
        eprintln!("skipping redis test because REDIS_URL is not set");
        return;
    };
    let backend = RedisBackend::<E>::new(client);
    let keep = keyspace::<E>(namespace("keep"));
    let drop = keyspace::<E>(namespace("drop"));

    backend
        .put(
            &keep,
            &1,
            &E {
                id: 1,
                label: "keep".into(),
            },
            None,
        )
        .await
        .unwrap();
    backend
        .put(
            &drop,
            &1,
            &E {
                id: 1,
                label: "drop".into(),
            },
            None,
        )
        .await
        .unwrap();

    backend.invalidate_all(&drop).await.unwrap();

    assert_eq!(backend.get(&drop, &1).await.unwrap(), None);
    assert_eq!(backend.get(&keep, &1).await.unwrap().unwrap().label, "keep");
}

#[tokio::test]
async fn redis_invalidate_all_is_type_scoped() {
    let Some(client) = redis_client() else {
        eprintln!("skipping redis test because REDIS_URL is not set");
        return;
    };
    let e_backend = RedisBackend::<E>::new(client.clone());
    let f_backend = RedisBackend::<F>::new(client);
    let ns = namespace("type-scope");
    let e_keyspace = keyspace::<E>(ns.clone());
    let f_keyspace = keyspace::<F>(ns);

    e_backend
        .put(
            &e_keyspace,
            &1,
            &E {
                id: 1,
                label: "drop".into(),
            },
            None,
        )
        .await
        .unwrap();
    f_backend
        .put(
            &f_keyspace,
            &1,
            &F {
                id: 1,
                label: "keep".into(),
            },
            None,
        )
        .await
        .unwrap();

    e_backend.invalidate_all(&e_keyspace).await.unwrap();

    assert_eq!(e_backend.get(&e_keyspace, &1).await.unwrap(), None);
    assert_eq!(
        f_backend.get(&f_keyspace, &1).await.unwrap().unwrap().label,
        "keep"
    );
}
