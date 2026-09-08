//! The client's side of one connection: XADD, XRANGE and XREAD, which is
//! everything a Location needs to append and to read on from a cursor.

use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, TransportError, classify, protocol_error};
use transport::socket;

use crate::resp::{Value, encode, read};

/// One entry as the server returns it: its id and its field-value pairs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub id: String,
    pub fields: Vec<(String, Vec<u8>)>,
}

impl Entry {
    /// The value of `field`, or none.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&[u8]> {
        self.fields
            .iter()
            .find(|(f, _)| f == name)
            .map(|(_, v)| v.as_slice())
    }
}

pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    /// Connect to `server` and check it answers PING.
    ///
    /// # Errors
    /// Where the server could not be reached or did not answer PONG.
    pub fn connect(server: &str, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(server, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self { reader, writer };
        match client.call(&[b"PING"])? {
            Value::Simple(text) if text.eq_ignore_ascii_case("PONG") => Ok(client),
            other => Err(protocol_error(format!("PING answered {other:?}"))),
        }
    }

    /// Send one command and take its reply, an error reply as an error.
    ///
    /// # Errors
    /// Where the connection broke, or the server answered `-ERR`.
    pub fn call(&mut self, arguments: &[&[u8]]) -> Result<Value> {
        self.writer
            .write_all(&encode(&Value::command(arguments)))
            .map_err(|e| classify("writing a command", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a command", &e))?;
        match read(&mut self.reader)? {
            Some(Value::Error(text)) => Err(TransportError::permanent(format!(
                "the server answered {text}"
            ))),
            Some(value) => Ok(value),
            None => Err(protocol_error("the server closed before answering")),
        }
    }

    /// Append `fields` to `stream`; the id the server assigned.
    ///
    /// # Errors
    /// Where the server refused or went away.
    pub fn add(&mut self, stream: &str, fields: &[(&str, &[u8])]) -> Result<String> {
        let mut arguments: Vec<&[u8]> = vec![b"XADD", stream.as_bytes(), b"*"];
        for (name, value) in fields {
            arguments.push(name.as_bytes());
            arguments.push(value);
        }
        self.call(&arguments)?
            .as_text()
            .ok_or_else(|| protocol_error("XADD did not answer with an id"))
    }

    /// The entries of `stream` after `after`, `0-0` for all, at most `count`.
    ///
    /// # Errors
    /// Where the server refused or answered a shape that is not entries.
    pub fn read_after(&mut self, stream: &str, after: &str, count: usize) -> Result<Vec<Entry>> {
        let start = if after == "0-0" {
            "-".to_string()
        } else {
            format!("({after}")
        };
        let count = count.to_string();
        let reply = self.call(&[
            b"XRANGE",
            stream.as_bytes(),
            start.as_bytes(),
            b"+",
            b"COUNT",
            count.as_bytes(),
        ])?;
        entries(&reply)
    }
}

/// An XRANGE reply as entries.
fn entries(reply: &Value) -> Result<Vec<Entry>> {
    let shape = || protocol_error("a reply that is not a list of entries");
    reply
        .as_array()
        .ok_or_else(shape)?
        .iter()
        .map(|entry| {
            let parts = entry.as_array().ok_or_else(shape)?;
            let id = parts.first().and_then(Value::as_text).ok_or_else(shape)?;
            let pairs = parts.get(1).and_then(Value::as_array).ok_or_else(shape)?;
            let fields = pairs
                .chunks(2)
                .map(|pair| {
                    let name = pair.first().and_then(Value::as_text).ok_or_else(shape)?;
                    let value = pair.get(1).and_then(Value::as_bytes).ok_or_else(shape)?;
                    Ok((name, value.to_vec()))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Entry { id, fields })
        })
        .collect()
}
