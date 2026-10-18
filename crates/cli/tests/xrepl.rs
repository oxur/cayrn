//! Actual Rust consumer, including a controlling PTY and keyboard Ctrl-C.
#[path = "support/pty.rs"]
mod pty;
#[expect(
    dead_code,
    reason = "Harness utilities are shared by separate test binaries."
)]
mod support;
use pty::Terminal;
use serde_json::{Value, json};
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use support::*;

fn binary() -> PathBuf {
    let path = env::var_os("XREPL_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../../../lfe/xrepl/xrepl/target/debug/xrepl")
        });
    path.canonicalize()
        .expect("xrepl binary missing: build xrepl and/or set XREPL_BIN")
}

#[test]
fn json_consumer_replays_contiguous_events_and_eof_only_detaches() {
    let binary = binary();
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    evaluate(&mut c, &s, "(set cayrn-probe 43)");
    let events = transcript(&mut c, &s);
    let head = events.last().unwrap()["sequence"].as_str().unwrap();
    let out = h.root.path().join("xrepl.jsonl");
    let err = h.root.path().join("xrepl.stderr");
    let mut child = Process::spawn(
        Command::new(binary)
            .args([
                "--cayrn",
                h.state.to_str().unwrap(),
                "--session",
                s["id"].as_str().unwrap(),
                "--after",
                head,
                "--cayrn-json",
            ])
            .stdin(Stdio::piped())
            .stdout(fs::File::create(&out).unwrap())
            .stderr(fs::File::create(&err).unwrap()),
    );
    let mut input = child.0.stdin.take().unwrap();
    input.write_all(b"cayrn-probe\n").unwrap();
    drop(input);
    assert!(
        child.wait().success(),
        "{}",
        fs::read_to_string(err).unwrap()
    );
    let records: Vec<Value> = fs::read_to_string(out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records[0]["session"], s["id"]);
    assert!(records.iter().any(|e| e["event"]["value"] == "43"));
    let sequences: Vec<u64> = records
        .iter()
        .filter_map(|e| e["sequence"].as_str())
        .map(|v| v.parse().unwrap())
        .collect();
    assert!(!sequences.is_empty());
    assert_eq!(
        sequences,
        (head.parse::<u64>().unwrap() + 1..)
            .take(sequences.len())
            .collect::<Vec<_>>()
    );
    assert_eq!(state(&mut c, &s), "active");
    assert_eq!(evaluate(&mut c, &s, "cayrn-probe").1["value"], "43");
}

#[test]
fn draft_survives_peer_output_and_quit_keeps_live_state() {
    let binary = binary();
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    evaluate(&mut c, &s, "(set cayrn-probe 43)");
    let events = transcript(&mut c, &s);
    let head = events.last().unwrap()["sequence"].as_str().unwrap();
    let mut terminal = Terminal::spawn(&binary, &h.state, s["id"].as_str().unwrap(), head);
    terminal.collect(Duration::from_millis(800));
    terminal.write(b"(+ cayrn-probe ");
    terminal.collect(Duration::from_millis(200));
    evaluate(&mut c, &s, "(progn (io:format \"EDIT-BUFFER\" ()) 0)");
    let during = terminal.collect(Duration::from_millis(600));
    assert!(
        !during
            .windows(b"EDIT-BUFFER".len())
            .any(|w| w == b"EDIT-BUFFER"),
        "peer output overwrote the draft"
    );
    terminal.write(b"1)\r");
    terminal.until(b"Succeeded] 44");
    terminal.until(b"EDIT-BUFFER");
    assert!(
        transcript(&mut c, &s)
            .iter()
            .any(|e| e["event"]["submission"]["source"] == "(+ cayrn-probe 1)")
    );
    terminal.quit();
    assert_eq!(evaluate(&mut c, &s, "cayrn-probe").1["value"], "43");
}

#[test]
fn keyboard_ctrl_c_interrupts_evaluation_and_preserves_peer() {
    let binary = binary();
    let mut h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let peer = create(&mut c);
    evaluate(&mut c, &peer, "(set retained 43)");
    let mut terminal = Terminal::spawn(&binary, &h.state, s["id"].as_str().unwrap(), "0");
    terminal.collect(Duration::from_millis(800));
    terminal.write(b"(progn (io:format \"CTRL-C-START\" ()) (timer:sleep 10000) 42)\r");
    // Match captured output, not the echoed source, before sending the actual
    // keyboard byte to the controlling terminal (never substitute kill(SIGINT)).
    terminal.until(b"] CTRL-C-START");
    terminal.write(b"\x03");
    terminal.until(b"Cancelled]");
    wait_until("keyboard interruption state loss", || {
        state(&mut c, &s) == "state_lost"
    });
    let events = transcript(&mut c, &s);
    assert!(
        events
            .iter()
            .any(|e| e["event"]["kind"] == "outcome" && e["event"]["phase"] == "cancelled")
    );
    terminal.quit();
    assert!(h.agent.alive());
    assert_eq!(evaluate(&mut c, &peer, "retained").1["value"], "43");
    assert_eq!(
        c.data(json!({"op":"inspect_session","session":peer["id"]}))["state"],
        "active"
    );
}
