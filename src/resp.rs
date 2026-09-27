//! RESP2, the Redis serialization protocol: five types, each a type byte, a
//! line, and for bulk strings and arrays what the line said follows.

use std::io::BufRead;

use transport::ceiling;
use transport::error::{Result, classify, protocol_error};

/// One RESP value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// `+text`
    Simple(String),
    /// `-text`
    Error(String),
    /// `:number`
    Integer(i64),
    /// `$length` and the bytes, or `$-1` for null.
    Bulk(Option<Vec<u8>>),
    /// `*count` and the elements, or `*-1` for null.
    Array(Option<Vec<Value>>),
}

impl Value {
    /// A bulk string from text.
    #[must_use]
    pub fn bulk(text: &str) -> Self {
        Value::Bulk(Some(text.as_bytes().to_vec()))
    }

    /// A command: every argument a bulk string, as clients send them.
    #[must_use]
    pub fn command(arguments: &[&[u8]]) -> Self {
        Value::Array(Some(
            arguments
                .iter()
                .map(|a| Value::Bulk(Some(a.to_vec())))
                .collect(),
        ))
    }

    /// The bytes where this is a bulk string, or none.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bulk(Some(bytes)) => Some(bytes),
            _ => None,
        }
    }

    /// The text where this is a bulk or simple string, lossily.
    #[must_use]
    pub fn as_text(&self) -> Option<String> {
        match self {
            Value::Bulk(Some(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
            Value::Simple(text) => Some(text.clone()),
            _ => None,
        }
    }

    /// The elements where this is an array, or none.
    #[must_use]
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(Some(elements)) => Some(elements),
            _ => None,
        }
    }
}

/// `value` as bytes on the wire.
#[must_use]
pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write(&mut out, value);
    out
}

fn write(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Simple(text) => out.extend_from_slice(format!("+{text}\r\n").as_bytes()),
        Value::Error(text) => out.extend_from_slice(format!("-{text}\r\n").as_bytes()),
        Value::Integer(n) => out.extend_from_slice(format!(":{n}\r\n").as_bytes()),
        Value::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
        Value::Bulk(Some(bytes)) => {
            out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        Value::Array(None) => out.extend_from_slice(b"*-1\r\n"),
        Value::Array(Some(elements)) => {
            out.extend_from_slice(format!("*{}\r\n", elements.len()).as_bytes());
            for element in elements {
                write(out, element);
            }
        }
    }
}

/// The deepest arrays are nested in one value before it is refused: a reply
/// Redis writes nests four deep at most (XREAD), and a peer sending
/// `*1` without end would otherwise recurse until the stack overflows.
pub const MAX_DEPTH: usize = 32;

/// Read one value, or `None` when the peer closed between values.
///
/// # Errors
/// A connection that closes mid-value, a line over `net::read::MAX_LINE` or
/// not UTF-8, a type byte RESP does not have, a length that is not a number,
/// a bulk length over `net::MAX_BODY`, or arrays nested deeper than
/// [`MAX_DEPTH`].
pub fn read(reader: &mut impl BufRead) -> Result<Option<Value>> {
    nested(reader, 0)
}

/// One value `depth` arrays down.
fn nested(reader: &mut impl BufRead, depth: usize) -> Result<Option<Value>> {
    let Some(line) = net::read::line(reader)? else {
        return Ok(None);
    };
    let Some(&kind) = line.as_bytes().first() else {
        return Err(protocol_error("a RESP line without its type byte"));
    };
    let text = line.get(1..).unwrap_or_default().to_string();
    let value = match kind {
        b'+' => Value::Simple(text),
        b'-' => Value::Error(text),
        b':' => Value::Integer(number(&text)?),
        b'$' => match number(&text)? {
            -1 => Value::Bulk(None),
            length if length >= 0 => {
                let length = usize::try_from(length).unwrap_or(usize::MAX);
                ceiling::within(length, net::MAX_BODY, "Xmip reads in one bulk string")?;
                let mut bytes = vec![0u8; length + 2];
                reader
                    .read_exact(&mut bytes)
                    .map_err(|e| classify("reading a bulk string", &e))?;
                if !bytes.ends_with(b"\r\n") {
                    return Err(protocol_error("a bulk string not followed by CRLF"));
                }
                bytes.truncate(length);
                Value::Bulk(Some(bytes))
            }
            _ => return Err(protocol_error("a negative bulk length")),
        },
        b'*' => match number(&text)? {
            -1 => Value::Array(None),
            count if count >= 0 => {
                if depth == MAX_DEPTH {
                    return Err(protocol_error(format!(
                        "arrays nested deeper than the {MAX_DEPTH} Xmip reads"
                    )));
                }
                let mut elements = Vec::new();
                for _ in 0..count {
                    let element = nested(reader, depth + 1)?
                        .ok_or_else(|| protocol_error("the peer closed inside an array"))?;
                    elements.push(element);
                }
                Value::Array(Some(elements))
            }
            _ => return Err(protocol_error("a negative array length")),
        },
        other => {
            return Err(protocol_error(format!(
                "{:?} is not a RESP type byte",
                char::from(other)
            )));
        }
    };
    Ok(Some(value))
}

fn number(text: &str) -> Result<i64> {
    text.parse()
        .map_err(|_| protocol_error(format!("{text:?} is not a number")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: &Value) {
        let bytes = encode(value);
        let back = read(&mut bytes.as_slice()).expect("read").expect("one");
        assert_eq!(&back, value);
    }

    #[test]
    fn every_type_round_trips() {
        round_trip(&Value::Simple("OK".into()));
        round_trip(&Value::Error("ERR unknown".into()));
        round_trip(&Value::Integer(-42));
        round_trip(&Value::Bulk(None));
        round_trip(&Value::bulk(""));
        round_trip(&Value::Bulk(Some(b"bin\r\nary\0".to_vec())));
        round_trip(&Value::Array(None));
        round_trip(&Value::command(&[b"XADD", b"orders", b"*", b"body", b"x"]));
        round_trip(&Value::Array(Some(vec![
            Value::bulk("1-0"),
            Value::Array(Some(vec![Value::bulk("body"), Value::Integer(1)])),
        ])));
    }

    #[test]
    fn what_is_not_resp_is_refused() {
        assert!(read(&mut &b""[..]).expect("closed").is_none());
        assert!(read(&mut &b"?x\r\n"[..]).is_err(), "type byte");
        assert!(read(&mut &b"$x\r\n"[..]).is_err(), "length");
        assert!(read(&mut &b"$5\r\nab\r\n"[..]).is_err(), "short");
        assert!(read(&mut &b"$-2\r\n"[..]).is_err(), "negative");
        assert!(read(&mut &b"*2\r\n+a\r\n"[..]).is_err(), "closed in array");
        assert!(read(&mut &b"\r\n"[..]).is_err(), "no type byte");
        let deepest = [b"*1\r\n".repeat(MAX_DEPTH), b":1\r\n".to_vec()].concat();
        assert!(read(&mut deepest.as_slice()).is_ok(), "at the limit");
        let endless = b"*1\r\n".repeat(100_000);
        let refused = read(&mut endless.as_slice()).expect_err("nested past the limit");
        assert!(
            refused.message.contains("nested deeper"),
            "{}",
            refused.message
        );
        let claimed = read(&mut &b"$9223372036854775807\r\n"[..]);
        assert!(claimed.expect_err("claimed").message.contains("over the"));
        let command = Value::command(&[b"PING"]);
        assert_eq!(
            command.as_array().expect("array")[0].as_text().as_deref(),
            Some("PING")
        );
        assert_eq!(
            command.as_array().expect("array")[0].as_bytes(),
            Some(&b"PING"[..])
        );
    }
}
