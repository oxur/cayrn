//! Standalone local fixture host. Journal policy is shared with cayrn-core;
//! each session has a bounded queue, owning thread and separate fixture process.
mod journal;
mod runtime;

use cayrn_core::{Admission, Journal, execute};
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

struct Job {
    submission: Submission,
    reply: mpsc::SyncSender<Result<Operation>>,
}
struct Worker {
    sender: Option<mpsc::SyncSender<Job>>,
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.sender.take();
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
    program: PathBuf,
    stopping: AtomicBool,
}
impl Service {
    fn handle(&self, request: Request) -> Result<Response> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(Fault::new(ErrorCode::Unavailable, "host is stopping"));
        }
        match request {
            Request::Describe => Ok(Response::Description {
                version: VERSION,
                backend: "fixture-only".into(),
                operations: [
                    "describe",
                    "create",
                    "inspect_session",
                    "submit",
                    "inspect_operation",
                    "read",
                ]
                .map(str::to_owned)
                .to_vec(),
            }),
            Request::Create => {
                let state = self.journal.create()?;
                let (sender, receiver) = mpsc::sync_channel::<Job>(128);
                let (ready_tx, ready_rx) = mpsc::sync_channel(1);
                let program = self.program.clone();
                let journal = self.journal.clone();
                let sid = state.id.clone();
                let stop = Arc::new(AtomicBool::new(false));
                let stopped = stop.clone();
                let join = thread::Builder::new()
                    .name(format!("cayrn-{}", state.id))
                    .spawn(move || {
                        let mut runtime = match ProcessRuntime::spawn(&program) {
                            Ok(runtime) => {
                                if ready_tx.send(Ok(())).is_err() {
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
                        for job in receiver {
                            if stopped.load(Ordering::Acquire) {
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
                                && let Err(error) = execute(&journal, &mut runtime, &job.submission)
                            {
                                eprintln!("session quarantined: {error}");
                                break;
                            }
                        }
                    })?;
                let worker = Worker {
                    sender: Some(sender),
                    stop,
                    join: Some(join),
                };
                ready_rx.recv().map_err(|_| {
                    Fault::new(ErrorCode::Unavailable, "runtime owner did not initialize")
                })??;
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
        let directory = directory.as_ref().to_owned();
        let program = std::fs::canonicalize(worker.as_ref())?;
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
                program,
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
