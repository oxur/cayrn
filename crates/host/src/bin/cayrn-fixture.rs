//! Controlled test worker; this is not an adapter for a real language.
use cayrn_protocol::WorkerFrame;
use std::io::{self, BufRead, Write};
fn send(frame: WorkerFrame) -> io::Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();
    serde_json::to_writer(&mut out, &frame)?;
    writeln!(out)?;
    out.flush()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut counter = 0_i64;
    for line in io::stdin().lock().lines() {
        let source: String = serde_json::from_str(&line?)?;
        if source == "crash" {
            std::process::exit(23);
        }
        let result = if source == "get" {
            Ok(counter.to_string())
        } else if let Some(n) = source.strip_prefix("add ") {
            n.parse::<i64>()
                .map_err(|_| "invalid integer".to_owned())
                .and_then(|n| {
                    counter
                        .checked_add(n)
                        .ok_or_else(|| "counter overflow".to_owned())
                })
                .map(|n| {
                    counter = n;
                    n.to_string()
                })
        } else if let Some(text) = source.strip_prefix("echo ") {
            send(WorkerFrame::Output(text.to_owned()))?;
            Ok("nil".to_owned())
        } else if let Some(n) = source.strip_prefix("sleep ") {
            match n.parse::<u64>() {
                Ok(ms) if ms <= 1000 => {
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                    Ok(counter.to_string())
                }
                _ => Err("sleep requires 0..1000 milliseconds".to_owned()),
            }
        } else {
            Err("fixture supports get, add N, echo TEXT, sleep MS, crash".to_owned())
        };
        send(match result {
            Ok(v) => WorkerFrame::Value(v),
            Err(e) => WorkerFrame::Error(e),
        })?;
    }
    Ok(())
}
