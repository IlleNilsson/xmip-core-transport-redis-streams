#![forbid(unsafe_code)]

//! Streams that arrive as Redis Stream entries. One entry is one Stream, the
//! stream key and the entry id kept beside it.
//!
//! A Redis Stream is an append-only log with ids that order it, on port
//! 6379: XADD appends, XRANGE and XREAD read on from an id, and the id a
//! Location last read is its cursor. A Receive Location reads the entries
//! after its cursor; a Send Location appends. Either may instead accept a
//! producer or consumer directly through [`Session`], one client's worth of
//! server over streams kept in memory.
//!
//! An entry is fields; this transport carries the Stream as one field,
//! `body` unless configured otherwise, and reads that field back. Consumer
//! groups — XREADGROUP, XACK, XPENDING — are how several nodes share one
//! stream and arrive with the routing capability's placement, problem 17.
//!
//! The origin URI carries what the server knew:
//! `redis://server/orders/1699999999999-0`.

pub mod client;
pub mod resp;
pub mod session;

use std::net::TcpListener;
use std::sync::Mutex;
use std::time::Duration;

pub use client::{Client, Entry};
pub use resp::Value;
pub use session::Session;
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

pub struct RedisStreamsTransport {
    server: String,
    stream: String,
    field: String,
    batch: usize,
    cursor: Mutex<String>,
    timeout: Option<Duration>,
}

impl Clone for RedisStreamsTransport {
    /// The same server, stream and field, and the cursor as it stands now
    /// — a copy reads on from where this one has got to, not from the
    /// beginning.
    fn clone(&self) -> Self {
        Self {
            server: self.server.clone(),
            stream: self.stream.clone(),
            field: self.field.clone(),
            batch: self.batch,
            cursor: Mutex::new(self.cursor()),
            timeout: self.timeout,
        }
    }
}

impl RedisStreamsTransport {
    /// Speak to the server at `server` about the stream `stream`, reading
    /// from its beginning.
    #[must_use]
    pub fn new(server: impl Into<String>, stream: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            stream: stream.into(),
            field: "body".to_string(),
            batch: 100,
            cursor: Mutex::new("0-0".to_string()),
            timeout: None,
        }
    }

    /// Carry the Stream in this field rather than `body`.
    #[must_use]
    pub fn in_field(mut self, field: impl Into<String>) -> Self {
        self.field = field.into();
        self
    }

    /// Start reading after this id rather than from the beginning.
    #[must_use]
    pub fn after(self, id: impl Into<String>) -> Self {
        *self
            .cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = id.into();
        self
    }

    /// Give up on a server that stops mid-reply.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The id the next receive reads after.
    #[must_use]
    pub fn cursor(&self) -> String {
        self.cursor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Connect to the server.
    ///
    /// # Errors
    /// Where the server could not be reached or does not speak RESP.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.server, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.server)
    }

    /// Accept one client on an already-bound listener.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.timeout)
    }

    /// Where a target names the server and stream itself —
    /// `redis://host:6379/orders` — or is a stream alone on this
    /// transport's server.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match socket::target("redis", target) {
            Some((peer, "")) => (peer, &self.stream),
            Some(pair) => pair,
            None => (&self.server, target),
        }
    }
}

impl Transport for RedisStreamsTransport {
    fn name(&self) -> &'static str {
        "redis-streams"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// The entries after the cursor, the cursor moved to the last of them.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        let entries = client.read_after(&self.stream, &self.cursor(), self.batch)?;
        let mut arrived = Vec::with_capacity(entries.len());
        for entry in entries {
            let body = entry.field(&self.field).unwrap_or(&[]).to_vec();
            arrived.push(Arrived::new(
                format!("redis://{}/{}/{}", self.server, self.stream, entry.id),
                body,
            ));
            *self
                .cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = entry.id;
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, stream) = self.resolve(target);
        let mut client = Client::connect(server, self.timeout)?;
        client.add(stream, &[(&self.field, bytes)]).map(|_| ())
    }
}

impl RedisStreamsTransport {
    /// Both ends on this machine: an ephemeral local port, the stream
    /// `probe`, the loopback timeout on every read.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for RedisStreamsTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        self.accept_one(listener)?
            .next_add()?
            .ok_or_else(|| protocol_error("the client closed without appending"))
    }
}

impl Loopback for RedisStreamsTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self::new(address, &self.stream)
            .in_field(&self.field)
            .timing_out_after(LOOPBACK_TIMEOUT)
            .send(&self.stream, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::edge_payloads;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn the_loopback_appends_one_entry_and_takes_it() {
        let arrived = RedisStreamsTransport::loopback()
            .round(b"entry")
            .expect("round");
        assert_eq!(arrived.bytes, b"entry");
        assert!(arrived.origin_uri.starts_with("redis://127.0.0.1:"));
        assert!(arrived.origin_uri.ends_with("/probe/1-0"));
        let long = vec![0x2a; 100_000];
        assert_eq!(
            RedisStreamsTransport::loopback()
                .round(&long)
                .expect("long")
                .bytes,
            long
        );
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let transport = RedisStreamsTransport::loopback();
        assert!(transport.ceiling().is_none());
        for (name, bytes) in edge_payloads() {
            assert!(transport.refuses(&bytes).is_none(), "{name}");
            let arrived = transport
                .round(&bytes)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(arrived.bytes, bytes, "{name}");
        }
    }

    #[test]
    fn a_producer_appends_to_a_session_and_a_consumer_reads_on_from_its_cursor() {
        let far_end = RedisStreamsTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let near =
                RedisStreamsTransport::new(address.clone(), "orders").timing_out_after(secs(2));
            near.send("orders", b"first")?;
            near.send(&format!("redis://{address}/orders"), b"second\r\n\0")?;
            let first = near.receive()?;
            let cursor = near.cursor();
            let rest = near.receive()?;
            Ok::<_, transport::TransportError>((first, cursor, rest))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        let one = session.next_add().expect("first").expect("an entry");
        assert_eq!(one.bytes, b"first");
        assert!(one.origin_uri.ends_with("/orders/1-0"));
        assert!(session.next_add().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener).expect("second");
        let two = session.next_add().expect("second").expect("an entry");
        assert_eq!(two.bytes, b"second\r\n\0");
        assert!(session.next_add().expect("closed").is_none());
        // Each session keeps its own streams, so the two receives that
        // follow read from fresh sessions and find nothing; a server is what
        // makes what one connection appended visible to the next.
        for _ in 0..2 {
            let mut session = far_end.accept_one(&listener).expect("a reader");
            assert!(session.next_add().expect("serving").is_none());
        }
        let (first, cursor, rest) = near.join().expect("thread").expect("round trip");
        assert!(first.is_empty(), "a fresh session holds nothing");
        assert_eq!(cursor, "0-0");
        assert!(rest.is_empty());
    }

    #[test]
    fn entries_read_back_from_the_session_that_holds_them() {
        let far_end = RedisStreamsTransport::new("127.0.0.1:0", "orders").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let near = std::thread::spawn(move || {
            let mut client = Client::connect(&address, Some(secs(2)))?;
            client.add("orders", &[("body", b"a"), ("kind", b"x")])?;
            client.add("orders", &[("body", b"b")])?;
            client.add("other", &[("body", b"c")])?;
            let all = client.read_after("orders", "0-0", 10)?;
            let after_first = client.read_after("orders", &all[0].id, 10)?;
            let one = client.read_after("orders", "0-0", 1)?;
            let xread = client.call(&[b"XREAD", b"COUNT", b"5", b"STREAMS", b"orders", b"1-0"])?;
            let length = client.call(&[b"XLEN", b"orders"])?;
            let unknown = client.call(&[b"FLUSHALL"]);
            Ok::<_, transport::TransportError>((all, after_first, one, xread, length, unknown))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        while session.next_add().expect("serving").is_some() {}
        let (all, after_first, one, xread, length, unknown) =
            near.join().expect("thread").expect("client");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].field("body"), Some(&b"a"[..]));
        assert_eq!(all[0].field("kind"), Some(&b"x"[..]));
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].field("body"), Some(&b"b"[..]));
        assert_eq!(one.len(), 1);
        assert_eq!(length, Value::Integer(2));
        let streams = xread.as_array().expect("streams");
        assert_eq!(streams.len(), 1);
        assert_eq!(
            streams[0].as_array().expect("pair")[0].as_text().as_deref(),
            Some("orders")
        );
        assert!(!unknown.expect_err("unknown").retryable);
        assert!(
            RedisStreamsTransport::new("127.0.0.1:0", "x")
                .claims()
                .is_none()
        );
    }
}
