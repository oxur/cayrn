//! Owned processes, bounded wire I/O and isolated state for real-runtime tests.
use serde_json::{Value, json};
use std::{
    env, fs,
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use uuid::Uuid;

const FRAME_LIMIT: usize = 256 * 1024;
pub const WAIT: Duration = Duration::from_secs(6);

/// Always reap only the process this guard started, including on assertion panic.
pub struct Process(pub Child);
impl Process {
    pub fn spawn(command: &mut Command) -> Self {
        Self(command.spawn().expect("spawn test-owned process"))
    }
    pub fn alive(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }
    pub fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "child {} did not exit",
                self.0.id()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
    pub fn signal(&mut self, signal: &str) -> ExitStatus {
        assert!(self.alive(), "cannot signal an exited process");
        // No shell or process-name matching: signal only our still-owned child PID.
        assert!(
            Command::new("kill")
                .args([signal, &self.0.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        self.wait()
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        // Cleanup is best-effort on panic, but errors remain visible. Explicit
        // lifecycle assertions above verify graceful exit where it is the oracle.
        match self.0.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                if let Err(error) = self.0.kill() {
                    eprintln!("test cleanup kill: {error}");
                }
            }
            Err(error) => eprintln!("test cleanup status: {error}"),
        }
        if let Err(error) = self.0.wait() {
            eprintln!("test cleanup reap: {error}");
        }
    }
}

pub struct Harness {
    // Field order drops children before their temporary files.
    pub host: Process,
    pub agent: Process,
    pub root: TempDir,
    pub endpoint: PathBuf,
    pub state: PathBuf,
}
impl Harness {
    pub fn new() -> Self {
        let agent_dir = env::var_os("CAYRN_LFE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../lfe/cayrn-lfe")
            });
        let launcher = agent_dir
            .join("bin/cayrn-lfe")
            .canonicalize()
            .expect("cayrn-lfe launcher missing: set CAYRN_LFE_DIR and build the native project");
        assert!(
            agent_dir
                .join("_build/default/lib/cayrn_lfe/ebin/cayrn_lfe.app")
                .exists(),
            "build cayrn-lfe first: make -C {} build",
            agent_dir.display()
        );
        let root = tempfile::Builder::new()
            .prefix("cayrn-native-")
            .tempdir_in("/tmp")
            .unwrap();
        let endpoint = root.path().join("agent.sock");
        let state = root.path().join("state");
        let log = fs::File::create(root.path().join("agent.log")).unwrap();
        let mut agent = Process::spawn(
            Command::new(launcher)
                .current_dir(root.path())
                // Dedicated test nodes use a small scheduler budget even when Cargo
                // runs independent scenarios concurrently. Never mutate global env.
                .env("ERL_FLAGS", "+S 2:2 +SDcpu 1 +SDio 1")
                .arg(&endpoint)
                .stdout(log.try_clone().unwrap())
                .stderr(log),
        );
        wait_socket(&endpoint, &mut agent);
        let host = start_host(&state, &endpoint, root.path());
        Self {
            host,
            agent,
            root,
            endpoint,
            state,
        }
    }
    pub fn client(&self) -> Wire {
        Wire::connect(&self.state.join("cayrn.sock"))
    }
    pub fn restart_host(&mut self) {
        self.host = start_host(&self.state, &self.endpoint, self.root.path());
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        if thread::panicking() {
            for name in ["agent.log", "host.log"] {
                eprintln!(
                    "{name}:\n{}",
                    fs::read_to_string(self.root.path().join(name))
                        .unwrap_or_else(|e| e.to_string())
                );
            }
        }
    }
}
fn start_host(state: &Path, endpoint: &Path, root: &Path) -> Process {
    let log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("host.log"))
        .unwrap();
    let mut host = Process::spawn(
        Command::new(env!("CARGO_BIN_EXE_cayrn"))
            .current_dir(root)
            .args(["serve-agent", "--state-dir"])
            .arg(state)
            .arg("--agent-socket")
            .arg(endpoint)
            .stdout(log.try_clone().unwrap())
            .stderr(log),
    );
    wait_socket(&state.join("cayrn.sock"), &mut host);
    host
}
fn wait_socket(path: &Path, process: &mut Process) {
    let deadline = Instant::now() + WAIT;
    loop {
        if UnixStream::connect(path).is_ok() {
            return;
        }
        if !process.alive() {
            for name in ["agent.log", "host.log"] {
                // The agent socket lives one level above the host socket.
                for root in [path.parent(), path.parent().and_then(Path::parent)]
                    .into_iter()
                    .flatten()
                {
                    if let Ok(log) = fs::read_to_string(root.join(name)) {
                        eprintln!("{name}:\n{log}");
                    }
                }
            }
            panic!("process exited before socket {} was ready", path.display());
        }
        assert!(
            Instant::now() < deadline,
            "socket not ready: {}",
            path.display()
        );
        thread::sleep(Duration::from_millis(10));
    }
}

/// Raw RPC preserves errors as values and allows intentionally losing a reply.
pub struct Wire(pub UnixStream);
impl Wire {
    pub fn connect(path: &Path) -> Self {
        let stream = UnixStream::connect(path).unwrap();
        stream.set_read_timeout(Some(WAIT)).unwrap();
        stream.set_write_timeout(Some(WAIT)).unwrap();
        Self(stream)
    }
    pub fn call(&mut self, request: Value) -> Value {
        let id = Uuid::new_v4().to_string();
        self.send(&json!({"version":1,"id":id,"request":request}));
        let reply = self.receive();
        assert_eq!(reply["version"], 1);
        assert_eq!(reply["id"], id);
        reply["response"].clone()
    }
    pub fn data(&mut self, request: Value) -> Value {
        let response = self.call(request);
        assert_ne!(response["type"], "error", "{response}");
        response["data"].clone()
    }
    pub fn send(&mut self, value: &Value) {
        let bytes = serde_json::to_vec(value).unwrap();
        assert!(bytes.len() <= FRAME_LIMIT);
        self.0
            .write_all(&u32::try_from(bytes.len()).unwrap().to_be_bytes())
            .unwrap();
        self.0.write_all(&bytes).unwrap();
    }
    pub fn receive(&mut self) -> Value {
        let mut header = [0; 4];
        self.0.read_exact(&mut header).unwrap();
        let size = u32::from_be_bytes(header) as usize;
        assert!(size <= FRAME_LIMIT, "oversized response");
        let mut bytes = vec![0; size];
        self.0.read_exact(&mut bytes).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
    pub fn expect_eof(&mut self) {
        let mut byte = [0];
        match self.0.read(&mut byte) {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            other => panic!("invalid native command did not close connection: {other:?}"),
        }
    }
}

pub fn create(c: &mut Wire) -> Value {
    c.data(json!({"op":"create"}))
}
pub fn submission(s: &Value, source: &str, operation: &str) -> Value {
    json!({"session":s["id"],"generation":s["generation"],"operation":operation,"client":"test","source":source})
}
pub fn submit(c: &mut Wire, sub: &Value) -> Value {
    let response = c.call(json!({"op":"submit","submission":sub}));
    assert_eq!(response["type"], "operation", "{response}");
    response["data"].clone()
}
pub fn operation(c: &mut Wire, s: &Value, id: &str) -> Value {
    c.data(json!({"op":"inspect_operation","session":s["id"],"operation":id}))
}
pub fn state(c: &mut Wire, s: &Value) -> Value {
    c.data(json!({"op":"inspect_session","session":s["id"]}))["state"].clone()
}
pub fn evaluate(c: &mut Wire, s: &Value, source: &str) -> (Value, Value) {
    evaluate_as(c, s, source, &Uuid::new_v4().to_string())
}
pub fn evaluate_as(c: &mut Wire, s: &Value, source: &str, id: &str) -> (Value, Value) {
    let sub = submission(s, source, id);
    submit(c, &sub);
    wait_until("operation terminal outcome", || {
        !matches!(
            operation(c, s, id)["phase"].as_str(),
            Some("queued" | "dispatched")
        )
    });
    (sub, operation(c, s, id))
}
pub fn transcript(c: &mut Wire, s: &Value) -> Vec<Value> {
    let mut after = "0".to_owned();
    let mut events = Vec::new();
    let deadline = Instant::now() + WAIT;
    loop {
        let page = c.data(json!({"op":"read","session":s["id"],"after":after,"limit":3}));
        for event in page["events"].as_array().unwrap() {
            let sequence = event["sequence"].as_str().unwrap().parse::<u64>().unwrap();
            assert_eq!(sequence, events.len() as u64 + 1, "journal cursor gap");
            events.push(event.clone());
        }
        let next = page["next"].as_str().unwrap().to_owned();
        if page["next"] == page["head"] {
            return events;
        }
        assert_ne!(after, next, "non-advancing read page");
        assert!(Instant::now() < deadline, "transcript read deadline");
        after = next;
    }
}
pub fn wait_until(label: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    loop {
        if predicate() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out: {label}");
        thread::sleep(Duration::from_millis(10));
    }
}
pub fn has_output(c: &mut Wire, s: &Value, text: &str) -> bool {
    transcript(c, s).iter().any(|e| e["event"]["text"] == text)
}
