use cayrn_core::{Outcome, Runtime};
use cayrn_protocol::{ErrorCode, Fault, MAX_FRAME, Result, WorkerFrame};
use std::{
    io::{BufRead, BufReader, Read, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

pub(crate) struct ProcessRuntime {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
}
impl ProcessRuntime {
    pub(crate) fn spawn(program: &Path) -> Result<Self> {
        let mut child = Command::new(program)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| Fault::new(ErrorCode::RuntimeLost, "missing worker stdin"))?;
        let output = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| Fault::new(ErrorCode::RuntimeLost, "missing worker stdout"))?,
        );
        Ok(Self {
            child,
            input,
            output,
        })
    }
}
impl Runtime for ProcessRuntime {
    fn evaluate(
        &mut self,
        source: &str,
        emit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<Outcome> {
        let source = serde_json::to_string(source)
            .map_err(|e| Fault::new(ErrorCode::Protocol, e.to_string()))?;
        writeln!(self.input, "{source}")?;
        self.input.flush()?;
        loop {
            let mut line = String::new();
            // Bound an internal worker frame as well as the public socket frame.
            let n = self
                .output
                .by_ref()
                .take((MAX_FRAME + 1) as u64)
                .read_line(&mut line)?;
            if n == 0 || n > MAX_FRAME || !line.ends_with('\n') {
                return Err(Fault::new(
                    ErrorCode::RuntimeLost,
                    "worker exited or emitted an invalid frame; outcome unknown",
                ));
            }
            let frame: WorkerFrame = serde_json::from_str(&line)
                .map_err(|e| Fault::new(ErrorCode::RuntimeLost, e.to_string()))?;
            match frame {
                WorkerFrame::Output(text) => emit(&text)?,
                WorkerFrame::Value(value) => {
                    if value.len() > cayrn_protocol::MAX_SOURCE {
                        return Err(Fault::new(
                            ErrorCode::RuntimeLost,
                            "worker value exceeds capture limit",
                        ));
                    }
                    return Ok(Outcome::Value(value));
                }
                WorkerFrame::Error(error) => {
                    if error.len() > cayrn_protocol::MAX_SOURCE {
                        return Err(Fault::new(
                            ErrorCode::RuntimeLost,
                            "worker error exceeds capture limit",
                        ));
                    }
                    return Ok(Outcome::Error(error));
                }
            }
        }
    }
}
impl Drop for ProcessRuntime {
    fn drop(&mut self) {
        // Ownership is explicit: shutting down this fixture context kills/reaps
        // only its dedicated process, never another session or shared runtime.
        if let Err(e) = self.child.kill()
            && e.kind() != std::io::ErrorKind::InvalidInput
        {
            eprintln!("fixture worker stop: {e}");
        }
        if let Err(e) = self.child.wait() {
            eprintln!("fixture worker reap: {e}");
        }
    }
}
