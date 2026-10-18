//! A real controlling terminal; the library owns platform-specific unsafe setup.
use crate::support::{WAIT, wait_until};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::{
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub struct Terminal {
    child: Box<dyn Child + Send + Sync>,
    master: Option<Box<dyn MasterPty + Send>>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    received: Option<mpsc::Receiver<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
    pub screen: Vec<u8>,
}
impl Terminal {
    pub fn spawn(binary: &Path, state: &Path, session: &str, after: &str) -> Self {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(binary);
        command.args([
            "--cayrn",
            state.to_str().unwrap(),
            "--session",
            session,
            "--after",
            after,
        ]);
        command.env("TERM", "xterm-256color");
        let child = pair.slave.spawn_command(command).unwrap();
        drop(pair.slave);
        let writer = Arc::new(Mutex::new(pair.master.take_writer().unwrap()));
        let mut source = pair.master.try_clone_reader().unwrap();
        let (send, received) = mpsc::sync_channel(128);
        let responder = writer.clone();
        let reader = thread::spawn(move || {
            let mut bytes = [0; 8192];
            let mut tail = Vec::new();
            loop {
                let size = match source.read(&mut bytes) {
                    Ok(0) => break,
                    Ok(size) => size,
                    // Unix PTYs commonly return EIO when the last slave closes.
                    Err(error) if error.raw_os_error() == Some(5) => break,
                    Err(error) => {
                        eprintln!("PTY reader: {error}");
                        break;
                    }
                };
                tail.extend_from_slice(&bytes[..size]);
                while let Some(at) = tail.windows(4).position(|w| w == b"\x1b[6n") {
                    if let Err(error) = responder.lock().unwrap().write_all(b"\x1b[1;1R") {
                        eprintln!("PTY cursor response: {error}");
                        return;
                    }
                    tail.drain(..at + 4);
                }
                if tail.len() > 3 {
                    tail.drain(..tail.len() - 3);
                }
                if send.send(bytes[..size].to_vec()).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            master: Some(pair.master),
            writer,
            received: Some(received),
            reader: Some(reader),
            screen: Vec::new(),
        }
    }
    pub fn write(&mut self, bytes: &[u8]) {
        self.writer.lock().unwrap().write_all(bytes).unwrap();
    }
    pub fn collect(&mut self, duration: Duration) -> Vec<u8> {
        let deadline = Instant::now() + duration;
        let mut output = Vec::new();
        while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            match self.received.as_ref().unwrap().recv_timeout(remaining) {
                Ok(chunk) => output.extend_from_slice(&chunk),
                Err(_) => break,
            }
        }
        self.screen.extend_from_slice(&output);
        output
    }
    pub fn until(&mut self, expected: &[u8]) {
        let deadline = Instant::now() + WAIT;
        while !self.screen.windows(expected.len()).any(|w| w == expected) {
            self.collect(Duration::from_millis(50));
            assert!(
                Instant::now() < deadline,
                "missing {:?} in PTY output:\n{}",
                String::from_utf8_lossy(expected),
                String::from_utf8_lossy(&self.screen)
            );
        }
    }
    pub fn quit(&mut self) {
        self.write(b":quit\r");
        let mut status = None;
        wait_until("terminal exit", || {
            self.collect(Duration::from_millis(20));
            status = self.child.try_wait().unwrap();
            status.is_some()
        });
        assert!(
            status.unwrap().success(),
            "{}",
            String::from_utf8_lossy(&self.screen)
        );
    }
}
impl Drop for Terminal {
    fn drop(&mut self) {
        if thread::panicking() {
            eprintln!("PTY screen:\n{}", String::from_utf8_lossy(&self.screen));
        }
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            _ => {
                if let Err(e) = self.child.kill() {
                    eprintln!("PTY cleanup kill: {e}");
                }
                if let Err(e) = self.child.wait() {
                    eprintln!("PTY cleanup reap: {e}");
                }
            }
        }
        // Disconnect the bounded receiver before joining so a full channel can
        // never strand the reader during panic cleanup. Closing the slave above
        // makes the terminal reader reach EOF/EIO; no detached helper thread.
        self.received.take();
        self.master.take();
        if let Some(reader) = self.reader.take()
            && reader.join().is_err()
        {
            eprintln!("PTY reader panicked");
        }
    }
}
