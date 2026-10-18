//! Standalone local fixture host. Journal policy is shared with cayrn-core;
//! each session has a bounded queue, owning thread and separate fixture process.
mod agent;
mod journal;
mod runtime;

use cayrn_core::{Admission, Journal, Outcome, Runtime, execute};
use cayrn_protocol::{
    Call, ErrorCode, Fault, Operation, Reply, Request, Response, Result, SessionState, Submission,
    VERSION, validate_id, wire,
};
use journal::SqliteJournal;
use runtime::ProcessRuntime;
use std::{
    collections::HashMap,
    future::Future,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::{Semaphore, watch},
    task::JoinSet,
};

#[derive(Clone)]
enum Backend {
    Fixture(PathBuf),
    Agent(PathBuf),
}
enum Execution {
    Fixture(ProcessRuntime),
    Agent(agent::AgentRuntime),
}
impl Runtime for Execution {
    fn evaluate(
        &mut self,
        source: &str,
        emit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<Outcome> {
        match self {
            Self::Fixture(r) => r.evaluate(source, emit),
            Self::Agent(r) => r.evaluate(source, emit),
        }
    }
}
impl Execution {
    fn prepare(&mut self, sub: &Submission) -> Result<()> {
        match self {
            Self::Fixture(_) => Ok(()),
            Self::Agent(r) => r.prepare(sub),
        }
    }
}
struct Job {
    submission: Submission,
    reply: mpsc::SyncSender<Result<Operation>>,
}
struct Worker {
    sender: Option<mpsc::SyncSender<Job>>,
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    control: Option<agent::Control>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.sender.take();
        if let Some(control) = &self.control {
            control.stop();
        }
        if let Some(join) = self.join.take()
            && join.join().is_err()
        {
            eprintln!("session worker panicked during teardown");
        }
    }
}
struct Service {
    journal: SqliteJournal,
    workers: Mutex<HashMap<String, Worker>>,
    backend: Backend,
    authority: String,
    stopping: AtomicBool,
}
impl Service {
    fn control(&self, sid: &str) -> Result<agent::Control> {
        self.workers
            .lock()
            .map_err(|_| Fault::new(ErrorCode::Unavailable, "worker registry poisoned"))?
            .get(sid)
            .and_then(|w| w.control.clone())
            .ok_or_else(|| Fault::new(ErrorCode::Unsupported, "native session control unavailable"))
    }

    fn handle(&self, request: Request) -> Result<Response> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Fault::new(ErrorCode::Unavailable, "host is stopping"));
        }
        match request {
            Request::Describe => Ok(Response::Description {
                version: VERSION,
                backend: match self.backend {
                    Backend::Fixture(_) => "fixture-only",
                    Backend::Agent(_) => "native-agent",
                }
                .into(),
                operations: [
                    "describe",
                    "create",
                    "inspect_session",
                    "submit",
                    "inspect_operation",
                    "read",
                    "follow",
                    "cancel",
                    "close",
                ]
                .map(str::to_owned)
                .to_vec(),
            }),
            Request::Cancel { session, operation } => {
                validate_id(&session)?;
                validate_id(&operation)?;
                let op = self.journal.lookup(&session, &operation)?;
                if !matches!(
                    op.phase,
                    cayrn_protocol::Phase::Queued | cayrn_protocol::Phase::Dispatched
                ) {
                    return Ok(Response::Control {
                        disposition: "already_terminal".into(),
                    });
                }
                let control = self.control(&session)?;
                let disposition = match control.interrupt {
                    cayrn_protocol::agent::InterruptMode::Unsupported => {
                        return Err(Fault::new(
                            ErrorCode::Unsupported,
                            "agent cannot interrupt this context",
                        ));
                    }
                    cayrn_protocol::agent::InterruptMode::Cooperative => {
                        "interrupt_requested_cooperative"
                    }
                    cayrn_protocol::agent::InterruptMode::SessionDestructive => {
                        "interrupt_requested_session_destructive"
                    }
                };
                control.command(&cayrn_protocol::agent::Command::Cancel { operation })?;
                Ok(Response::Control {
                    disposition: disposition.into(),
                })
            }
            Request::Close { session } => {
                validate_id(&session)?;
                let previous = self.journal.inspect(&session)?;
                if matches!(previous.state, SessionState::Closing | SessionState::Closed) {
                    return Ok(Response::Session(previous));
                }
                let control = self.control(&session)?;
                if !control.close_barrier {
                    return Err(Fault::new(
                        ErrorCode::Unsupported,
                        "agent does not provide a managed close barrier",
                    ));
                }
                let state = self.journal.begin_close(&session)?;
                control.command(&cayrn_protocol::agent::Command::Close)?;
                Ok(Response::Session(state))
            }
            Request::Create => {
                let state = self.journal.create()?;
                let (sender, receiver) = mpsc::sync_channel::<Job>(128);
                let (ready_tx, ready_rx) = mpsc::sync_channel(1);
                let backend = self.backend.clone();
                let authority = self.authority.clone();
                let context = state.clone();
                let journal = self.journal.clone();
                let sid = state.id.clone();
                let stop = Arc::new(AtomicBool::new(false));
                let stopped = stop.clone();
                let join = thread::Builder::new()
                    .name(format!("cayrn-{}", state.id))
                    .spawn(move || {
                        let opened = match backend {
                            Backend::Fixture(program) => {
                                ProcessRuntime::spawn(&program).map(Execution::Fixture)
                            }
                            Backend::Agent(socket) => agent::AgentRuntime::connect(
                                &socket,
                                &context,
                                &authority,
                                journal.clone(),
                            )
                            .map(Execution::Agent),
                        };
                        let mut runtime = match opened {
                            Ok(runtime) => {
                                let control = match &runtime {
                                    Execution::Agent(r) => Some(r.control()),
                                    _ => None,
                                };
                                if ready_tx.send(Ok(control)).is_err() {
                                    return;
                                }
                                runtime
                            }
                            Err(error) => {
                                if let Err(e) = journal.lose_runtime(&sid, &error.to_string()) {
                                    eprintln!("runtime creation journal failure: {e}");
                                }
                                let _ = ready_tx.send(Err(error));
                                return;
                            }
                        };
                        loop {
                            let job=match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                                Ok(job)=>job,
                                Err(mpsc::RecvTimeoutError::Timeout)=>{
                                    if matches!(&runtime,Execution::Agent(r) if r.control().stopped()) { break; }
                                    continue;
                                },
                                Err(mpsc::RecvTimeoutError::Disconnected)=>break,
                            };
                            if stopped.load(Ordering::Acquire) || matches!(&runtime,Execution::Agent(r) if r.control().stopped()) {
                                break;
                            }
                            let admission = journal.admit(&job.submission);
                            let run = matches!(admission, Ok(Admission::New(_)));
                            let reply = admission.map(|admit| match admit {
                                Admission::New(op) | Admission::Existing(op) => op,
                            });
                            // A vanished client is a lost observer, not cancellation.
                            let _ = job.reply.send(reply);
                            if run
                                && let Err(error) = runtime
                                    .prepare(&job.submission)
                                    .and_then(|()| execute(&journal, &mut runtime, &job.submission))
                            {
                                eprintln!("session quarantined: {error}");
                                break;
                            }
                        }
                    })?;
                let control = ready_rx.recv().map_err(|_| {
                    Fault::new(ErrorCode::Unavailable, "runtime owner did not initialize")
                })??;
                let worker = Worker {
                    sender: Some(sender),
                    stop,
                    join: Some(join),
                    control,
                };
                let mut workers = self
                    .workers
                    .lock()
                    .map_err(|_| Fault::new(ErrorCode::Unavailable, "session registry poisoned"))?;
                if self.stopping.load(Ordering::Acquire) {
                    drop(worker);
                    return Err(Fault::new(
                        ErrorCode::Unavailable,
                        "host stopped during session creation",
                    ));
                }
                // Reap completed owners rather than retain an in-memory registry
                // entry for every historical failed session. Records stay in SQLite.
                workers.retain(|_, worker| {
                    !worker.join.as_ref().is_some_and(|join| join.is_finished())
                });
                workers.insert(state.id.clone(), worker);
                Ok(Response::Session(state))
            }
            Request::InspectSession { session } => {
                validate_id(&session)?;
                Ok(Response::Session(self.journal.inspect(&session)?))
            }
            Request::InspectOperation { session, operation } => {
                validate_id(&session)?;
                validate_id(&operation)?;
                Ok(Response::Operation(
                    self.journal.lookup(&session, &operation)?,
                ))
            }
            Request::Follow {
                session,
                after,
                limit,
                wait_ms,
            } => {
                validate_id(&session)?;
                if wait_ms > 1000 {
                    return Err(Fault::new(
                        ErrorCode::InvalidRequest,
                        "follow wait exceeds 1000ms",
                    ));
                }
                let deadline = std::time::Instant::now()
                    + std::time::Duration::from_millis(u64::from(wait_ms));
                loop {
                    let page = self.journal.read(&session, after, limit)?;
                    if !page.events.is_empty() || std::time::Instant::now() >= deadline {
                        return Ok(Response::Events(page));
                    }
                    if self.stopping.load(Ordering::Acquire) {
                        return Err(Fault::new(ErrorCode::Unavailable, "host is stopping"));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
            Request::Read {
                session,
                after,
                limit,
            } => {
                validate_id(&session)?;
                Ok(Response::Events(self.journal.read(&session, after, limit)?))
            }
            Request::Submit { submission } => {
                submission.validate()?;
                match self
                    .journal
                    .lookup(&submission.session, &submission.operation)
                {
                    Ok(op) if op.submission == submission => return Ok(Response::Operation(op)),
                    Ok(_) => {
                        return Err(Fault::new(
                            ErrorCode::Conflict,
                            "operation ID belongs to a different submission",
                        ));
                    }
                    Err(e) if e.code == ErrorCode::NotFound => {}
                    Err(e) => return Err(e),
                }
                if self.journal.inspect(&submission.session)?.state != SessionState::Active {
                    return Err(Fault::new(
                        ErrorCode::StateLost,
                        "session runtime state was lost",
                    ));
                }
                let (reply_tx, reply_rx) = mpsc::sync_channel(1);
                {
                    let workers = self.workers.lock().map_err(|_| {
                        Fault::new(ErrorCode::Unavailable, "session registry poisoned")
                    })?;
                    let worker = workers.get(&submission.session).ok_or_else(|| {
                        Fault::new(ErrorCode::StateLost, "no live execution context")
                    })?;
                    worker
                        .sender
                        .as_ref()
                        .ok_or_else(|| Fault::new(ErrorCode::Unavailable, "session owner stopped"))?
                        .try_send(Job {
                            submission,
                            reply: reply_tx,
                        })
                        .map_err(|e| {
                            Fault::new(
                                ErrorCode::Busy,
                                format!("session admission queue unavailable: {e}"),
                            )
                        })?;
                }
                Ok(Response::Operation(reply_rx.recv().map_err(|_| {
                    Fault::new(ErrorCode::Unavailable, "session stopped before acceptance")
                })??))
            }
        }
    }
}

/// Bound local listener and owned service. Live sessions outlast socket clients.
pub struct Host {
    listener: UnixListener,
    service: Arc<Service>,
    socket: PathBuf,
}
impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}
impl Host {
    /// Acquire exclusive directory ownership, recover the journal and bind a socket.
    /// The worker executable must implement the internal fixture framing.
    pub async fn bind(directory: impl AsRef<Path>, worker: impl AsRef<Path>) -> Result<Self> {
        Self::bind_backend(
            directory.as_ref(),
            Backend::Fixture(std::fs::canonicalize(worker.as_ref())?),
        )
        .await
    }
    /// Connect each new session to the shared native-agent endpoint.
    pub async fn bind_agent(directory: impl AsRef<Path>, socket: impl AsRef<Path>) -> Result<Self> {
        Self::bind_backend(
            directory.as_ref(),
            Backend::Agent(std::fs::canonicalize(socket.as_ref())?),
        )
        .await
    }
    async fn bind_backend(directory: &Path, backend: Backend) -> Result<Self> {
        let directory = directory.to_owned();
        let (journal, directory) =
            tokio::task::spawn_blocking(move || SqliteJournal::open(&directory))
                .await
                .map_err(|e| Fault::new(ErrorCode::Unavailable, e.to_string()))??;
        let socket = directory.join("cayrn.sock");
        if socket.exists() {
            if !std::fs::symlink_metadata(&socket)?.file_type().is_socket() {
                return Err(Fault::new(
                    ErrorCode::Conflict,
                    "socket path occupied by a non-socket file",
                ));
            }
            std::fs::remove_file(&socket)?;
        }
        let listener = UnixListener::bind(&socket)?;
        Ok(Self {
            listener,
            service: Arc::new(Service {
                journal,
                workers: Mutex::new(HashMap::new()),
                backend,
                authority: uuid::Uuid::new_v4().to_string(),
                stopping: AtomicBool::new(false),
            }),
            socket,
        })
    }
    /// Actual socket path, useful to launch independent clients.
    pub fn socket_path(&self) -> &Path {
        &self.socket
    }
    /// Serve until shutdown; disconnects alone never trigger shutdown.
    /// Graceful shutdown is administrative host termination, not session close.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> Result<()> {
        let mut tasks = JoinSet::new();
        let slots = Arc::new(Semaphore::new(64));
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::pin!(shutdown);
        let result = loop {
            tokio::select! {
                ()=&mut shutdown=>break Ok(()),
                connection=self.listener.accept()=>{
                    let (stream,_)=match connection { Ok(c)=>c,Err(e)=>break Err(Fault::from(e)) };
                    let permit=match slots.clone().try_acquire_owned() { Ok(p)=>p,Err(_)=>{drop(stream);continue;} };
                    let service=self.service.clone();
                    let stop=stop_rx.clone();
                    tasks.spawn(async move {
                        let _permit=permit;
                        if let Err(e)=connection_loop(stream,service,stop).await
                            && e.kind()!=std::io::ErrorKind::UnexpectedEof
                            && e.kind()!=std::io::ErrorKind::BrokenPipe {
                            eprintln!("client connection: {e}");
                        }
                    });
                },
                Some(done)=tasks.join_next(),if !tasks.is_empty()=>{if let Err(e)=done { eprintln!("connection task failed: {e}"); }}
            }
        };
        self.service.stopping.store(true, Ordering::Release);
        if let Ok(workers) = self.service.workers.lock() {
            for worker in workers.values() {
                worker.stop.store(true, Ordering::Release);
                if let Some(control) = &worker.control {
                    control.stop();
                }
            }
        }
        let _ = stop_tx.send(true);
        while let Some(done) = tasks.join_next().await {
            if let Err(error) = done {
                eprintln!("connection shutdown: {error}");
            }
        }
        drop(self.listener);
        let socket = self.socket;
        tokio::task::spawn_blocking(move || {
            drop(self.service);
            std::fs::remove_file(socket)
        })
        .await
        .map_err(|e| Fault::new(ErrorCode::Unavailable, e.to_string()))??;
        result
    }
}
async fn connection_loop(
    mut stream: UnixStream,
    service: Arc<Service>,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let call: Call = tokio::select! {
            _=stop.changed()=>return Ok(()),
            call=wire::read(&mut stream)=>call?,
        };
        let response = if call.version != VERSION {
            Response::Error(Fault::new(
                ErrorCode::Protocol,
                "unsupported protocol version",
            ))
        } else if let Err(e) = validate_id(&call.id) {
            Response::Error(e)
        } else {
            let service = service.clone();
            match tokio::task::spawn_blocking(move || service.handle(call.request)).await {
                Ok(Ok(reply)) => reply,
                Ok(Err(error)) => Response::Error(error),
                Err(e) => Response::Error(Fault::new(ErrorCode::Unavailable, e.to_string())),
            }
        };
        let reply = Reply {
            version: VERSION,
            id: call.id,
            response,
        };
        tokio::select! {
            _=stop.changed()=>return Ok(()),
            written=wire::write(&mut stream,&reply)=>written?,
        }
    }
}
