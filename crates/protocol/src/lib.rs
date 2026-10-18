//! Prototype semantic types. Cursors are strings on the wire; IDs are opaque.
use serde::{Deserialize, Serialize};
use std::fmt;

/// Prototype protocol version, not a released stable specification.
pub const VERSION: u16 = 1;
/// Maximum encoded payload; framing adds exactly four big-endian bytes.
pub const MAX_FRAME: usize = 262_144;
/// Maximum evaluated source bytes for the fixture profile.
pub const MAX_SOURCE: usize = 8192;
/// Maximum rows in a read page; byte budget can make pages smaller.
pub const MAX_PAGE: u16 = 64;
/// Maximum serialized event bytes in one read page.
pub const PAGE_BYTES: usize = 131_072;

/// An exact per-session event position, serialized as a canonical decimal string.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Cursor(pub u64);
impl TryFrom<String> for Cursor {
    type Error = String;
    fn try_from(value: String) -> std::result::Result<Self, String> {
        let n = value.parse::<u64>().map_err(|_| "invalid cursor")?;
        if n.to_string() != value {
            return Err("noncanonical cursor".into());
        }
        Ok(Self(n))
    }
}
impl From<Cursor> for String {
    fn from(value: Cursor) -> Self {
        value.0.to_string()
    }
}
/// Machine-readable failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    InvalidRequest,
    NotFound,
    Conflict,
    StaleGeneration,
    StateLost,
    Busy,
    Storage,
    RuntimeLost,
    Unavailable,
    Protocol,
    Io,
}
/// A bounded public error; arbitrary evaluated-code source is not added as context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fault {
    pub code: ErrorCode,
    pub message: String,
}
impl Fault {
    /// Construct a typed public fault.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}
impl std::error::Error for Fault {}
impl From<std::io::Error> for Fault {
    fn from(value: std::io::Error) -> Self {
        Self::new(ErrorCode::Io, value.to_string())
    }
}
/// Result at the common library/protocol boundary.
pub type Result<T> = std::result::Result<T, Fault>;
/// Durable session usability; no heap restoration is implied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Active,
    StateLost,
}
/// Stable session identity and its live-state generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub generation: String,
    pub state: SessionState,
}
/// Semantic submission identity; all fields participate in duplicate detection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submission {
    pub session: String,
    pub generation: String,
    pub operation: String,
    pub client: String,
    pub source: String,
}
impl Submission {
    /// Reject oversized source or ambiguous/unbounded identifiers before admission.
    pub fn validate(&self) -> Result<()> {
        for id in [
            &self.session,
            &self.generation,
            &self.operation,
            &self.client,
        ] {
            validate_id(id)?;
        }
        if self.source.len() > MAX_SOURCE {
            return Err(Fault::new(
                ErrorCode::InvalidRequest,
                "source exceeds 8192 bytes",
            ));
        }
        Ok(())
    }
}
/// Validate an opaque identifier in the initial local profile.
pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Fault::new(
            ErrorCode::InvalidRequest,
            "identifier must be 1..128 ASCII letters, digits, hyphens or underscores",
        ));
    }
    Ok(())
}
/// Durable operation phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Queued,
    Dispatched,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}
/// A durable operation record; value is a bounded textual fixture preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub submission: Submission,
    pub phase: Phase,
    pub value: Option<String>,
}
/// Replayable observation. Late output and completion are separate event kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    SessionCreated,
    Accepted {
        submission: Submission,
    },
    DispatchIntent {
        operation: String,
    },
    Output {
        operation: String,
        text: String,
    },
    Outcome {
        operation: String,
        phase: Phase,
        value: Option<String>,
    },
    StateLost {
        reason: String,
    },
}
/// One immutable event at an exact session cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub session: String,
    pub generation: String,
    pub sequence: Cursor,
    pub event: EventKind,
}
/// A bounded replay page; use next as the following read's after cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    pub events: Vec<Event>,
    pub next: Cursor,
    pub head: Cursor,
}
/// Initial protocol operation vocabulary; unsupported future operations fail decoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Describe,
    Create,
    InspectSession {
        session: String,
    },
    Submit {
        submission: Submission,
    },
    InspectOperation {
        session: String,
        operation: String,
    },
    /// Wait up to wait_ms (maximum 1000) for a bounded committed page.
    Follow {
        session: String,
        after: Cursor,
        limit: u16,
        wait_ms: u16,
    },
    Read {
        session: String,
        after: Cursor,
        limit: u16,
    },
}
/// Response payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum Response {
    Description {
        version: u16,
        backend: String,
        operations: Vec<String>,
    },
    Session(Session),
    Operation(Operation),
    Events(Page),
    Error(Fault),
}
/// Correlation/version envelope. Operation identity is separate from this ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call {
    pub version: u16,
    pub id: String,
    pub request: Request,
}
/// Correlated reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub version: u16,
    pub id: String,
    pub response: Response,
}
/// Internal fixture-worker messages, independent of the public session journal.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "text", rename_all = "snake_case")]
pub enum WorkerFrame {
    Output(String),
    Value(String),
    Error(String),
}

/// Internal native-agent protocol, independently versioned from client RPC.
pub mod agent {
    use super::{Cursor, Phase};
    use serde::{Deserialize, Serialize};
    /// Current experimental native-agent version.
    pub const VERSION: u16 = 1;
    /// Host-to-agent control and execution; no evaluated-language fields.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
    pub enum Command {
        Hello {
            version: u16,
            session: String,
            generation: String,
            authority: String,
        },
        Dispatch {
            operation: String,
            source: String,
            generation: String,
            authority: String,
        },
        Ack {
            position: Cursor,
        },
        Stop,
    }
    /// Agent-to-host capture and outcomes. Position is a connection-local exact cursor.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
    pub enum Event {
        Ready {
            version: u16,
            backend: String,
        },
        Output {
            operation: String,
            position: Cursor,
            text: String,
        },
        Outcome {
            operation: String,
            phase: Phase,
            value: String,
        },
    }
}

/// Shared, single-layer bounded JSON transport framing.
#[cfg(feature = "wire")]
pub mod wire {
    use super::MAX_FRAME;
    use serde::{Serialize, de::DeserializeOwned};
    use std::io;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    /// Read one bounded payload. Incomplete/malformed frames fail the connection.
    pub async fn read<R: AsyncRead + Unpin, T: DeserializeOwned>(input: &mut R) -> io::Result<T> {
        let size = input.read_u32().await? as usize;
        if size == 0 || size > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid frame length",
            ));
        }
        let mut bytes = vec![0; size];
        input.read_exact(&mut bytes).await?;
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
    /// Encode/frame exactly once and flush one bounded payload.
    pub async fn write<W: AsyncWrite + Unpin, T: Serialize>(
        output: &mut W,
        value: &T,
    ) -> io::Result<()> {
        let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
        if bytes.is_empty() || bytes.len() > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "outgoing frame too large",
            ));
        }
        output.write_u32(bytes.len() as u32).await?;
        output.write_all(&bytes).await?;
        output.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_preserves_numbers_beyond_javascript_integer_precision() {
        let cursor = Cursor(9_007_199_254_740_993);
        assert_eq!(
            serde_json::to_string(&cursor).unwrap(),
            "\"9007199254740993\""
        );
        assert_eq!(
            serde_json::from_str::<Cursor>("\"18446744073709551615\"").unwrap(),
            Cursor(u64::MAX)
        );
        for invalid in [
            "9007199254740993",
            "\"01\"",
            "\"-1\"",
            "\"+1\"",
            "\"18446744073709551616\"",
        ] {
            assert!(serde_json::from_str::<Cursor>(invalid).is_err());
        }
    }
}
