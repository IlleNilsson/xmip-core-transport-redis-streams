//! The server's side of one connection: what a test puts at the far end, and
//! what a Location that lets a producer XADD straight into Xmip runs.
//!
//! Not Redis. One session serves one client over streams kept in memory:
//! `XADD`, `XLEN`, `XRANGE`, `XREAD` without blocking, `PING`. What is
//! appended is handed up as a Stream; what was appended before is served
//! back. Consumer groups, blocking reads and persistence are a server's.

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify};
use transport::socket;

use crate::resp::{Value, encode, read};

type Fields = Vec<(Vec<u8>, Vec<u8>)>;

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    streams: BTreeMap<String, Vec<(String, Fields)>>,
    next_id: u64,
}

impl Session {
    /// Accept one client on `listener`.
    ///
    /// # Errors
    /// Where the connection could not be accepted.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        Ok(Self {
            reader,
            writer,
            peer,
            streams: BTreeMap::new(),
            next_id: 0,
        })
    }

    /// The next entry the client appends, or `None` when it closed. Reads
    /// and pings are answered on the way. The Stream is the entry's first
    /// field value; the origin names the stream and the id.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_add(&mut self) -> Result<Option<Arrived>> {
        loop {
            let Some(command) = read(&mut self.reader)? else {
                return Ok(None);
            };
            let arguments: Vec<Vec<u8>> = command
                .as_array()
                .unwrap_or(&[])
                .iter()
                .filter_map(|a| a.as_bytes().map(<[u8]>::to_vec))
                .collect();
            let verb = arguments
                .first()
                .map(|v| String::from_utf8_lossy(v).to_ascii_uppercase())
                .unwrap_or_default();
            let text = |i: usize| {
                arguments
                    .get(i)
                    .map(|a| String::from_utf8_lossy(a).into_owned())
                    .unwrap_or_default()
            };
            match verb.as_str() {
                "PING" => self.reply(&Value::Simple("PONG".into()))?,
                "XADD" if arguments.len() >= 5 && arguments.len() % 2 == 1 => {
                    let key = text(1);
                    self.next_id += 1;
                    let id = if text(2) == "*" {
                        format!("{}-0", self.next_id)
                    } else {
                        text(2)
                    };
                    let fields: Fields = arguments[3..]
                        .chunks(2)
                        .map(|pair| (pair[0].clone(), pair[1].clone()))
                        .collect();
                    let body = fields[0].1.clone();
                    self.streams
                        .entry(key.clone())
                        .or_default()
                        .push((id.clone(), fields));
                    self.reply(&Value::bulk(&id))?;
                    let origin = format!("redis://{}/{key}/{id}", self.peer);
                    return Ok(Some(Arrived::new(origin, body)));
                }
                "XLEN" => {
                    let length = self.streams.get(&text(1)).map_or(0, Vec::len);
                    self.reply(&Value::Integer(i64::try_from(length).unwrap_or(0)))?;
                }
                "XRANGE" if arguments.len() >= 4 => {
                    let count = if arguments.len() >= 6 {
                        text(5).parse().unwrap_or(usize::MAX)
                    } else {
                        usize::MAX
                    };
                    let reply = self.range(&text(1), &text(2), &text(3), count);
                    self.reply(&reply)?;
                }
                "XREAD" => {
                    let reply = self.xread(&arguments);
                    self.reply(&reply)?;
                }
                _ => self.reply(&Value::Error(format!("ERR unknown command {verb:?}")))?,
            }
        }
    }

    /// Entries of `key` from `start` to `end`: `-`, `+`, an id, or `(id` for
    /// exclusive.
    fn range(&self, key: &str, start: &str, end: &str, count: usize) -> Value {
        let entries = self.streams.get(key).map_or(&[][..], Vec::as_slice);
        let (exclusive, start) = match start.strip_prefix('(') {
            Some(rest) => (true, rest),
            None => (false, start),
        };
        let after = |id: &str| match start {
            "-" => true,
            s => {
                if exclusive {
                    id_of(id) > id_of(s)
                } else {
                    id_of(id) >= id_of(s)
                }
            }
        };
        let before = |id: &str| end == "+" || id_of(id) <= id_of(end);
        Value::Array(Some(
            entries
                .iter()
                .filter(|(id, _)| after(id) && before(id))
                .take(count)
                .map(|(id, fields)| entry(id, fields))
                .collect(),
        ))
    }

    /// `XREAD [COUNT n] STREAMS key... id...`, answered without blocking.
    fn xread(&self, arguments: &[Vec<u8>]) -> Value {
        let words: Vec<String> = arguments
            .iter()
            .map(|a| String::from_utf8_lossy(a).into_owned())
            .collect();
        let mut count = usize::MAX;
        let mut at = 1;
        if words
            .get(at)
            .is_some_and(|w| w.eq_ignore_ascii_case("COUNT"))
        {
            count = words
                .get(at + 1)
                .and_then(|c| c.parse().ok())
                .unwrap_or(usize::MAX);
            at += 2;
        }
        if !words
            .get(at)
            .is_some_and(|w| w.eq_ignore_ascii_case("STREAMS"))
        {
            return Value::Error("ERR syntax error".into());
        }
        let names = &words[at + 1..];
        let half = names.len() / 2;
        let mut result = Vec::new();
        for (key, id) in names[..half].iter().zip(&names[half..]) {
            let start = format!("({id}");
            let entries = self.range(key, &start, "+", count);
            if entries.as_array().is_some_and(|e| !e.is_empty()) {
                result.push(Value::Array(Some(vec![Value::bulk(key), entries])));
            }
        }
        if result.is_empty() {
            Value::Array(None)
        } else {
            Value::Array(Some(result))
        }
    }

    fn reply(&mut self, value: &Value) -> Result<()> {
        self.writer
            .write_all(&encode(value))
            .map_err(|e| classify("writing a reply", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a reply", &e))
    }
}

/// `ms-seq` as a pair for ordering; `0-0` for anything that is not one.
fn id_of(id: &str) -> (u64, u64) {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    (ms.parse().unwrap_or(0), seq.parse().unwrap_or(0))
}

fn entry(id: &str, fields: &Fields) -> Value {
    let mut pairs = Vec::with_capacity(fields.len() * 2);
    for (name, value) in fields {
        pairs.push(Value::Bulk(Some(name.clone())));
        pairs.push(Value::Bulk(Some(value.clone())));
    }
    Value::Array(Some(vec![Value::bulk(id), Value::Array(Some(pairs))]))
}
