use cayrn_core::{Admission, Journal};
use cayrn_protocol::{
    Cursor, ErrorCode, Event, EventKind, Fault, Operation, PAGE_BYTES, Page, Phase, Result,
    Session, SessionState, Submission,
};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct SqliteJournal {
    inner: Arc<Inner>,
}
struct Inner {
    connection: Mutex<Connection>,
    _owner: File,
}
fn storage(error: impl std::fmt::Display) -> Fault {
    Fault::new(ErrorCode::Storage, error.to_string())
}
fn encode(value: &impl serde::Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(storage)
}
fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(storage)
}
fn session(db: &Connection, id: &str) -> Result<Session> {
    let json: Option<String> = db
        .query_row("SELECT json FROM sessions WHERE id=?1", [id], |r| r.get(0))
        .optional()
        .map_err(storage)?;
    decode(&json.ok_or_else(|| Fault::new(ErrorCode::NotFound, "session not found"))?)
}
fn operation(db: &Connection, sid: &str, id: &str) -> Result<Option<Operation>> {
    db.query_row(
        "SELECT json FROM operations WHERE session=?1 AND id=?2",
        params![sid, id],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(storage)?
    .map(|json| decode(&json))
    .transpose()
}
fn head(db: &Connection, sid: &str) -> Result<u64> {
    let n: i64 = db
        .query_row(
            "SELECT COALESCE(MAX(seq),0) FROM events WHERE session=?1",
            [sid],
            |r| r.get(0),
        )
        .map_err(storage)?;
    u64::try_from(n).map_err(storage)
}
fn append(db: &Connection, state: &Session, kind: EventKind) -> Result<()> {
    let seq = head(db, &state.id)?
        .checked_add(1)
        .and_then(|n| i64::try_from(n).ok())
        .ok_or_else(|| storage("event sequence exhausted"))?;
    let event = Event {
        session: state.id.clone(),
        generation: state.generation.clone(),
        sequence: Cursor(seq as u64),
        event: kind,
    };
    db.execute(
        "INSERT INTO events(session,seq,json) VALUES(?1,?2,?3)",
        params![state.id, seq, encode(&event)?],
    )
    .map_err(storage)?;
    Ok(())
}
fn save_operation(db: &Connection, op: &Operation) -> Result<()> {
    let updated = db
        .execute(
            "UPDATE operations SET json=?3 WHERE session=?1 AND id=?2",
            params![op.submission.session, op.submission.operation, encode(op)?],
        )
        .map_err(storage)?;
    if updated != 1 {
        return Err(storage("operation update lost its record"));
    }
    Ok(())
}
fn lose(db: &Connection, sid: &str, reason: &str) -> Result<()> {
    let mut state = session(db, sid)?;
    if state.state == SessionState::StateLost {
        return Ok(());
    }
    let operations = {
        let mut statement = db
            .prepare("SELECT json FROM operations WHERE session=?1 AND json_extract(json,'$.phase') IN ('queued','dispatched')")
            .map_err(storage)?;
        let rows = statement
            .query_map([sid], |r| r.get::<_, String>(0))
            .map_err(storage)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(storage)?
    };
    for json in operations {
        let mut op: Operation = decode(&json)?;
        op.phase = match op.phase {
            Phase::Queued => Phase::Cancelled,
            Phase::Dispatched => Phase::Unknown,
            _ => continue,
        };
        op.value = Some(reason.to_owned());
        save_operation(db, &op)?;
        append(
            db,
            &state,
            EventKind::Outcome {
                operation: op.submission.operation,
                phase: op.phase,
                value: op.value,
            },
        )?;
    }
    state.state = SessionState::StateLost;
    db.execute(
        "UPDATE sessions SET json=?2 WHERE id=?1",
        params![sid, encode(&state)?],
    )
    .map_err(storage)?;
    append(
        db,
        &state,
        EventKind::StateLost {
            reason: reason.to_owned(),
        },
    )
}
impl SqliteJournal {
    pub(crate) fn open(directory: &Path) -> Result<(Self, PathBuf)> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)?;
        let metadata = fs::symlink_metadata(directory)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(Fault::new(
                ErrorCode::InvalidRequest,
                "state directory must be a real directory with mode 0700 (or stricter)",
            ));
        }
        let directory = fs::canonicalize(directory)?;
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(directory.join("host.lock"))?;
        owner.try_lock().map_err(|e| {
            Fault::new(
                ErrorCode::Busy,
                format!("cannot acquire state-directory ownership: {e}"),
            )
        })?;
        let database = directory.join("journal.sqlite3");
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&database)?;
        let mut connection = Connection::open(database).map_err(storage)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(2))
            .map_err(storage)?;
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
            )
            .map_err(storage)?;
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .map_err(storage)?;
        match version {
            0 => connection.execute_batch("BEGIN IMMEDIATE;
                CREATE TABLE sessions(id TEXT PRIMARY KEY, json TEXT NOT NULL);
                CREATE TABLE operations(session TEXT NOT NULL REFERENCES sessions(id), id TEXT NOT NULL, json TEXT NOT NULL, PRIMARY KEY(session,id));
                CREATE TABLE events(session TEXT NOT NULL REFERENCES sessions(id), seq INTEGER NOT NULL, json TEXT NOT NULL, PRIMARY KEY(session,seq));
                PRAGMA user_version=1; COMMIT;").map_err(storage)?,
            1 => {},
            _ => return Err(storage("unsupported journal schema version")),
        }
        let tx = connection.transaction().map_err(storage)?;
        let sessions = {
            let mut statement = tx
                .prepare("SELECT id FROM sessions WHERE json_extract(json,'$.state')='active'")
                .map_err(storage)?;
            statement
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(storage)?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(storage)?
        };
        for sid in sessions {
            lose(
                &tx,
                &sid,
                "session host restarted; live runtime state was not restored",
            )?;
        }
        tx.commit().map_err(storage)?;
        Ok((
            Self {
                inner: Arc::new(Inner {
                    connection: Mutex::new(connection),
                    _owner: owner,
                }),
            },
            directory,
        ))
    }
    fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        self.inner
            .connection
            .lock()
            .map_err(|_| storage("journal owner poisoned"))
    }
    pub(crate) fn create(&self) -> Result<Session> {
        let mut db = self.db()?;
        let tx = db.transaction().map_err(storage)?;
        // Count in SQLite instead of loading historical session JSON into RAM.
        let active: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE json_extract(json,'$.state')='active'",
                [],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if active >= 64 {
            return Err(Fault::new(ErrorCode::Busy, "live session limit reached"));
        }
        let state = Session {
            id: Uuid::new_v4().to_string(),
            generation: Uuid::new_v4().to_string(),
            state: SessionState::Active,
        };
        tx.execute(
            "INSERT INTO sessions(id,json) VALUES(?1,?2)",
            params![state.id, encode(&state)?],
        )
        .map_err(storage)?;
        append(&tx, &state, EventKind::SessionCreated)?;
        tx.commit().map_err(storage)?;
        Ok(state)
    }
    pub(crate) fn inspect(&self, sid: &str) -> Result<Session> {
        let db = self.db()?;
        session(&db, sid)
    }
    pub(crate) fn lookup(&self, sid: &str, id: &str) -> Result<Operation> {
        let db = self.db()?;
        session(&db, sid)?;
        operation(&db, sid, id)?
            .ok_or_else(|| Fault::new(ErrorCode::NotFound, "operation not found"))
    }
    pub(crate) fn read(&self, sid: &str, after: Cursor, limit: u16) -> Result<Page> {
        let db = self.db()?;
        session(&db, sid)?;
        let last = head(&db, sid)?;
        if limit == 0 || limit > cayrn_protocol::MAX_PAGE || after.0 > last {
            return Err(Fault::new(
                ErrorCode::InvalidRequest,
                "invalid page limit or cursor beyond head",
            ));
        }
        let mut query = db
            .prepare("SELECT json FROM events WHERE session=?1 AND seq>?2 ORDER BY seq LIMIT ?3")
            .map_err(storage)?;
        let rows = query
            .query_map(params![sid, after.0 as i64, limit], |r| {
                r.get::<_, String>(0)
            })
            .map_err(storage)?;
        let mut events = Vec::new();
        let mut bytes = 0;
        for row in rows {
            let json = row.map_err(storage)?;
            if bytes + json.len() > PAGE_BYTES {
                break;
            }
            bytes += json.len();
            events.push(decode::<Event>(&json)?);
        }
        let next = events.last().map_or(after, |e| e.sequence);
        Ok(Page {
            events,
            next,
            head: Cursor(last),
        })
    }
}
impl Journal for SqliteJournal {
    fn admit(&self, sub: &Submission) -> Result<Admission> {
        sub.validate()?;
        let mut db = self.db()?;
        let tx = db.transaction().map_err(storage)?;
        if let Some(op) = operation(&tx, &sub.session, &sub.operation)? {
            if op.submission != *sub {
                return Err(Fault::new(
                    ErrorCode::Conflict,
                    "operation ID already belongs to a different submission",
                ));
            }
            return Ok(Admission::Existing(op));
        }
        let state = session(&tx, &sub.session)?;
        if state.state != SessionState::Active {
            return Err(Fault::new(
                ErrorCode::StateLost,
                "session runtime state was lost",
            ));
        }
        if state.generation != sub.generation {
            return Err(Fault::new(
                ErrorCode::StaleGeneration,
                "submission names a different runtime generation",
            ));
        }
        let op = Operation {
            submission: sub.clone(),
            phase: Phase::Queued,
            value: None,
        };
        tx.execute(
            "INSERT INTO operations(session,id,json) VALUES(?1,?2,?3)",
            params![sub.session, sub.operation, encode(&op)?],
        )
        .map_err(storage)?;
        append(
            &tx,
            &state,
            EventKind::Accepted {
                submission: sub.clone(),
            },
        )?;
        tx.commit().map_err(storage)?;
        Ok(Admission::New(op))
    }
    fn dispatch(&self, sub: &Submission) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction().map_err(storage)?;
        let state = session(&tx, &sub.session)?;
        let mut op = operation(&tx, &sub.session, &sub.operation)?
            .ok_or_else(|| storage("missing admitted operation"))?;
        if op.phase != Phase::Queued || state.state != SessionState::Active {
            return Err(Fault::new(
                ErrorCode::StateLost,
                "operation cannot dispatch in current state",
            ));
        }
        op.phase = Phase::Dispatched;
        save_operation(&tx, &op)?;
        append(
            &tx,
            &state,
            EventKind::DispatchIntent {
                operation: sub.operation.clone(),
            },
        )?;
        tx.commit().map_err(storage)
    }
    fn output(&self, sub: &Submission, text: &str) -> Result<()> {
        if text.len() > cayrn_protocol::MAX_SOURCE {
            return Err(Fault::new(
                ErrorCode::InvalidRequest,
                "fixture output exceeds capture limit",
            ));
        }
        let mut db = self.db()?;
        let tx = db.transaction().map_err(storage)?;
        append(
            &tx,
            &session(&tx, &sub.session)?,
            EventKind::Output {
                operation: sub.operation.clone(),
                text: text.to_owned(),
            },
        )?;
        tx.commit().map_err(storage)
    }
    fn finish(&self, sub: &Submission, phase: Phase, value: &str) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction().map_err(storage)?;
        let mut op = operation(&tx, &sub.session, &sub.operation)?
            .ok_or_else(|| storage("missing dispatched operation"))?;
        if op.phase != Phase::Dispatched {
            return Err(storage("outcome does not follow dispatch"));
        }
        op.phase = phase;
        op.value = Some(value.to_owned());
        save_operation(&tx, &op)?;
        append(
            &tx,
            &session(&tx, &sub.session)?,
            EventKind::Outcome {
                operation: sub.operation.clone(),
                phase,
                value: op.value,
            },
        )?;
        tx.commit().map_err(storage)
    }
    fn lose_runtime(&self, sid: &str, reason: &str) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction().map_err(storage)?;
        lose(&tx, sid, reason)?;
        tx.commit().map_err(storage)
    }
}
