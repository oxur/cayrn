//! Shared full-duplex connector. No evaluated-language semantics live here.
use crate::journal::SqliteJournal;
use cayrn_core::{Journal, Outcome, Runtime};
use cayrn_protocol::{
    ErrorCode, Fault, MAX_FRAME, Result, Session, Submission,
    agent::{self, Command, Event},
};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

fn fault(message: impl Into<String>) -> Fault {
    Fault::new(ErrorCode::RuntimeLost, message)
}
fn write(stream: &mut UnixStream, value: &Command) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|e| fault(e.to_string()))?;
    if bytes.len() > MAX_FRAME {
        return Err(fault("agent command exceeds frame limit"));
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    Ok(())
}
fn read(stream: &mut UnixStream) -> Result<Event> {
    let mut size = [0; 4];
    stream.read_exact(&mut size)?;
    let size = u32::from_be_bytes(size) as usize;
    if size == 0 || size > MAX_FRAME {
        return Err(fault("invalid agent frame length"));
    }
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|e| fault(e.to_string()))
}
#[derive(Debug, Clone)]
pub(crate) struct Control {
    socket: Arc<UnixStream>,
    stopped: Arc<AtomicBool>,
}
impl Control {
    pub(crate) fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
    pub(crate) fn stop(&self) {
        // Wakes blocked readers without waiting for a writer mutex.
        self.stopped.store(true, Ordering::Release);
        if let Err(e) = self.socket.shutdown(Shutdown::Both)
            && e.kind() != std::io::ErrorKind::NotConnected
        {
            eprintln!("agent shutdown: {e}");
        }
    }
}
pub(crate) struct AgentRuntime {
    writer: Arc<Mutex<UnixStream>>,
    control: Control,
    operations: Arc<Mutex<HashMap<String, Submission>>>,
    outcomes: mpsc::Receiver<Result<(String, Outcome)>>,
    reader: Option<thread::JoinHandle<()>>,
    generation: String,
    authority: String,
    current: Option<String>,
}
impl AgentRuntime {
    pub(crate) fn connect(
        path: &Path,
        state: &Session,
        authority: &str,
        journal: SqliteJournal,
    ) -> Result<Self> {
        let mut socket = UnixStream::connect(path)?;
        socket.set_read_timeout(Some(Duration::from_secs(3)))?;
        socket.set_write_timeout(Some(Duration::from_secs(3)))?;
        write(
            &mut socket,
            &Command::Hello {
                version: agent::VERSION,
                session: state.id.clone(),
                generation: state.generation.clone(),
                authority: authority.into(),
            },
        )?;
        match read(&mut socket)? {
            Event::Ready { version, backend }
                if version == agent::VERSION && !backend.is_empty() && backend.len() <= 128 => {}
            _ => return Err(fault("unsupported agent handshake")),
        }
        socket.set_read_timeout(None)?;
        let control = Control {
            socket: Arc::new(socket.try_clone()?),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        let writer = Arc::new(Mutex::new(socket.try_clone()?));
        let operations = Arc::new(Mutex::new(HashMap::<String, Submission>::new()));
        let mapped = operations.clone();
        let output = writer.clone();
        let (tx, outcomes) = mpsc::sync_channel(1);
        let sid = state.id.clone();
        let reader_control = control.clone();
        let reader = thread::Builder::new()
            .name(format!("cayrn-agent-{}", state.id))
            .spawn(move || {
                let result = (|| -> Result<()> {
                    let mut position = 0u64;
                    loop {
                        match read(&mut socket)? {
                            Event::Output {
                                operation,
                                position: next,
                                text,
                            } => {
                                if next.0
                                    != position
                                        .checked_add(1)
                                        .ok_or_else(|| fault("capture cursor exhausted"))?
                                {
                                    return Err(fault("agent capture discontinuity"));
                                }
                                let sub = mapped
                                    .lock()
                                    .map_err(|_| fault("operation mapping poisoned"))?
                                    .get(&operation)
                                    .cloned()
                                    .ok_or_else(|| fault("unknown output operation"))?;
                                journal.output(&sub, &text)?;
                                write(
                                    &mut *output.lock().map_err(|_| fault("writer poisoned"))?,
                                    &Command::Ack { position: next },
                                )?;
                                position = next.0;
                            }
                            Event::Outcome {
                                operation,
                                phase,
                                value,
                            } => {
                                if value.len() > cayrn_protocol::MAX_SOURCE {
                                    return Err(fault("agent value exceeds capture limit"));
                                }
                                let outcome = match phase {
                                    cayrn_protocol::Phase::Succeeded => Outcome::Value(value),
                                    cayrn_protocol::Phase::Failed => Outcome::Error(value),
                                    _ => return Err(fault("invalid native outcome phase")),
                                };
                                tx.try_send(Ok((operation, outcome)))
                                    .map_err(|_| fault("unsolicited or repeated native outcome"))?;
                            }
                            Event::Ready { .. } => return Err(fault("repeated native handshake")),
                        }
                    }
                })();
                if let Err(e) = result {
                    reader_control.stop();
                    if let Err(record) =
                        journal.lose_runtime(&sid, &format!("agent link/capture lost: {e}"))
                    {
                        eprintln!("agent loss journal: {record}");
                    }
                    let _ = tx.try_send(Err(e)); // Owner may already be stopping.
                }
            })?;
        Ok(Self {
            writer,
            control,
            operations,
            outcomes,
            reader: Some(reader),
            generation: state.generation.clone(),
            authority: authority.into(),
            current: None,
        })
    }
    pub(crate) fn control(&self) -> Control {
        self.control.clone()
    }
    pub(crate) fn prepare(&mut self, submission: &Submission) -> Result<()> {
        let mut operations = self
            .operations
            .lock()
            .map_err(|_| fault("operation mapping poisoned"))?;
        if operations.len() >= 256 {
            return Err(fault(
                "native context operation budget reached; create a fresh session",
            ));
        }
        operations.insert(submission.operation.clone(), submission.clone());
        self.current = Some(submission.operation.clone());
        Ok(())
    }
}
impl Runtime for AgentRuntime {
    fn evaluate(
        &mut self,
        source: &str,
        _emit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<Outcome> {
        let operation = self
            .current
            .take()
            .ok_or_else(|| fault("missing native dispatch identity"))?;
        write(
            &mut *self.writer.lock().map_err(|_| fault("writer poisoned"))?,
            &Command::Dispatch {
                operation: operation.clone(),
                source: source.into(),
                generation: self.generation.clone(),
                authority: self.authority.clone(),
            },
        )?;
        let (id, outcome) = self
            .outcomes
            .recv()
            .map_err(|_| fault("native reader stopped"))??;
        if id != operation {
            return Err(fault("native outcome correlation mismatch"));
        }
        Ok(outcome)
    }
}
impl Drop for AgentRuntime {
    fn drop(&mut self) {
        self.control.stop();
        if let Some(reader) = self.reader.take()
            && reader.join().is_err()
        {
            eprintln!("native reader panicked");
        }
    }
}
