//! Real LFE/BEAM tests. Explicitly enabled; missing prerequisites fail the run.
mod support;

use rusqlite::Connection;
use serde_json::json;
use std::{fs, io::Write, os::unix::process::ExitStatusExt, thread, time::Duration};
use support::*;
use uuid::Uuid;

#[test]
fn malformed_version_and_stale_native_frames_fail_closed() {
    let mut h = Harness::new();
    let mut bad = Wire::connect(&h.endpoint);
    bad.send(
        &json!({"kind":"hello","version":999,"session":"bad","generation":"g","authority":"a"}),
    );
    bad.expect_eof();
    for bytes in [&[0, 0, 0, 1, b'{'][..], &262145_u32.to_be_bytes()[..]] {
        let mut bad = Wire::connect(&h.endpoint);
        bad.0.write_all(bytes).unwrap();
        bad.expect_eof();
    }
    for field in ["generation", "authority"] {
        let mut bad = Wire::connect(&h.endpoint);
        bad.send(&json!({"kind":"hello","version":1,"session":Uuid::new_v4().to_string(),"generation":"g","authority":"a"}));
        assert_eq!(bad.receive()["kind"], "ready");
        let mut dispatch = json!({"kind":"dispatch","operation":"invalid","generation":"g","authority":"a","source":"42"});
        dispatch[field] = json!("stale");
        bad.send(&dispatch);
        bad.expect_eof();
    }
    assert!(h.agent.alive());
    let mut c = h.client();
    let s = create(&mut c);
    assert_eq!(evaluate(&mut c, &s, "(+ 20 22)").1["value"], "42");
}

#[test]
fn bindings_shared_clients_lost_reply_dedup_and_reconnect() {
    let h = Harness::new();
    let mut terminal = h.client();
    let mut editor = h.client();
    let s = create(&mut terminal);
    assert_eq!(
        evaluate(&mut terminal, &s, "(set cayrn-probe 41)").1["value"],
        "41"
    );
    assert_eq!(
        evaluate(&mut editor, &s, "(+ cayrn-probe 1)").1["value"],
        "42"
    );
    let (sub, result) = evaluate_as(
        &mut terminal,
        &s,
        "(set cayrn-probe (+ cayrn-probe 1))",
        "once",
    );
    assert_eq!(result["value"], "42");
    assert_eq!(submit(&mut editor, &sub), result);
    let mut changed = sub.clone();
    changed["source"] = json!("(set cayrn-probe 999)");
    assert_eq!(
        editor.call(json!({"op":"submit","submission":changed}))["data"]["code"],
        "conflict"
    );
    assert_eq!(evaluate(&mut terminal, &s, "cayrn-probe").1["value"], "42");
    let lost_sub = submission(&s, "(set cayrn-probe (+ cayrn-probe 1))", "lost-reply");
    let mut lost = h.client();
    lost.send(&json!({"version":1,"id":"lost","request":{"op":"submit","submission":lost_sub}}));
    drop(lost); // Intentionally lose admission reply; repeat the same identity.
    submit(&mut editor, &lost_sub);
    assert_eq!(evaluate(&mut terminal, &s, "cayrn-probe").1["value"], "43");
    assert_eq!(
        evaluate(
            &mut terminal,
            &s,
            "(progn (io:format \"hello ~ts\" (list \"世界\")) cayrn-probe)"
        )
        .1["phase"],
        "succeeded"
    );
    assert!(has_output(&mut terminal, &s, "hello 世界"));
    assert_eq!(transcript(&mut terminal, &s), transcript(&mut editor, &s));
    for source in ["(/ 1 0)", "("] {
        assert_eq!(evaluate(&mut terminal, &s, source).1["phase"], "failed");
    }
    assert_eq!(evaluate(&mut terminal, &s, "cayrn-probe").1["value"], "43");
    assert!(
        evaluate(&mut terminal, &s, "(io:get_line \"input?\")").1["value"]
            .as_str()
            .unwrap()
            .contains("enotsup")
    );
    let before = transcript(&mut terminal, &s);
    drop(terminal);
    drop(editor);
    let mut reconnect = h.client();
    assert_eq!(state(&mut reconnect, &s), "active");
    assert_eq!(evaluate(&mut reconnect, &s, "cayrn-probe").1["value"], "43");
    assert!(transcript(&mut reconnect, &s).starts_with(&before));
}

#[test]
fn busy_context_does_not_block_peer_or_inspection() {
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let peer = create(&mut c);
    let slow = submission(
        &s,
        "(progn (io:format \"BUSY-START\" ()) (timer:sleep 3000) 7)",
        "slow",
    );
    submit(&mut c, &slow);
    wait_until("slow evaluation started", || {
        has_output(&mut c, &s, "BUSY-START")
    });
    assert_eq!(evaluate(&mut c, &peer, "(+ 20 22)").1["value"], "42");
    assert_eq!(operation(&mut c, &s, "slow")["phase"], "dispatched");
    wait_until("slow evaluation completed", || {
        operation(&mut c, &s, "slow")["phase"] == "succeeded"
    });
}

#[test]
fn late_output_and_follow_preserve_origin_and_contiguous_cursors() {
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let (delayed, _) = evaluate(
        &mut c,
        &s,
        "(progn (spawn (lambda () (timer:sleep 150) (io:format \"LATE-ORIGIN\" ()))) (quote scheduled))",
    );
    let (following, _) = evaluate(&mut c, &s, "(timer:sleep 400)");
    let events = transcript(&mut c, &s);
    let late = events
        .iter()
        .find(|e| e["event"]["text"] == "LATE-ORIGIN")
        .unwrap();
    assert_eq!(late["event"]["operation"], delayed["operation"]);
    let outcome = events
        .iter()
        .find(|e| {
            e["event"]["kind"] == "outcome" && e["event"]["operation"] == following["operation"]
        })
        .unwrap();
    let number = |e: &serde_json::Value| e["sequence"].as_str().unwrap().parse::<u64>().unwrap();
    assert!(number(late) < number(outcome));
    let after = events.last().unwrap()["sequence"].clone();
    let mut observer = h.client();
    thread::scope(|scope| {
        let waiting = scope.spawn(|| {
            observer.data(
                json!({"op":"follow","session":s["id"],"after":after,"limit":64,"wait_ms":1000}),
            )
        });
        evaluate(&mut c, &s, "(+ 20 22)");
        let page = waiting.join().unwrap();
        assert_eq!(
            number(&page["events"][0]),
            after.as_str().unwrap().parse::<u64>().unwrap() + 1
        );
    });
}

#[test]
fn cancellation_confirms_death_blocks_side_effect_and_preserves_peer() {
    let mut h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let peer = create(&mut c);
    evaluate(&mut c, &peer, "(set retained 43)");
    let marker = h.root.path().join("cancel-marker");
    let source = format!(
        "(progn (io:format \"INTERRUPT-START\" ()) (timer:sleep 1000) (file:write_file \"{}\" \"bad\"))",
        marker.display()
    );
    submit(&mut c, &submission(&s, &source, "interrupt"));
    wait_until("interrupt evaluation started", || {
        has_output(&mut c, &s, "INTERRUPT-START")
    });
    assert_eq!(
        c.data(json!({"op":"cancel","session":s["id"],"operation":"interrupt"}))["disposition"],
        "interrupt_requested_session_destructive"
    );
    wait_until("interrupt context discarded", || {
        state(&mut c, &s) == "state_lost"
    });
    assert_eq!(operation(&mut c, &s, "interrupt")["phase"], "cancelled");
    thread::sleep(Duration::from_millis(1100));
    assert!(!marker.exists());
    assert!(h.agent.alive());
    assert_eq!(evaluate(&mut c, &peer, "retained").1["value"], "43");
}

#[test]
fn busy_close_seals_capture_retains_archive_and_preserves_peer() {
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let peer = create(&mut c);
    evaluate(&mut c, &peer, "(set retained 43)");
    let source = "(progn (spawn (lambda () (timer:sleep 700) (io:format \"AFTER-BARRIER\" ()))) (io:format \"CLOSE-START\" ()) (timer:sleep 3000) 42)";
    submit(&mut c, &submission(&s, source, "close-active"));
    wait_until("close evaluation started", || {
        has_output(&mut c, &s, "CLOSE-START")
    });
    assert_eq!(
        c.data(json!({"op":"close","session":s["id"]}))["state"],
        "closing"
    );
    wait_until("native close barrier", || state(&mut c, &s) == "closed");
    let sealed = transcript(&mut c, &s);
    assert_eq!(sealed.last().unwrap()["event"]["kind"], "session_closed");
    assert_eq!(operation(&mut c, &s, "close-active")["phase"], "cancelled");
    assert_eq!(
        c.data(json!({"op":"close","session":s["id"]}))["state"],
        "closed"
    );
    assert_eq!(
        c.call(json!({"op":"submit","submission":submission(&s,"42","after-close")}))["type"],
        "error"
    );
    thread::sleep(Duration::from_millis(800));
    assert_eq!(transcript(&mut c, &s), sealed);
    assert_eq!(evaluate(&mut c, &peer, "retained").1["value"], "43");
}

fn storage_failure(boundary: &str, expected_phase: &str) {
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let peer = create(&mut c);
    evaluate(&mut c, &peer, "(set retained 43)");
    let marker = h.root.path().join("storage-marker");
    let db = Connection::open(h.state.join("journal.sqlite3")).unwrap();
    db.execute_batch(&format!("CREATE TRIGGER injected_failure BEFORE INSERT ON events WHEN json_extract(NEW.json, '$.event.kind')='{boundary}' BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")).unwrap();
    let source = format!(
        "(progn (io:format \"STORE-BOUNDARY\" ()) (file:write_file \"{}\" \"bad\"))",
        marker.display()
    );
    let sub = submission(&s, &source, "store-failure");
    submit(&mut c, &sub);
    wait_until("storage failure freezes context", || {
        state(&mut c, &s) == "state_lost"
    });
    assert_eq!(
        operation(&mut c, &s, "store-failure")["phase"],
        expected_phase
    );
    thread::sleep(Duration::from_millis(100));
    assert!(!marker.exists());
    assert_eq!(submit(&mut c, &sub)["phase"], expected_phase);
    assert!(!has_output(&mut c, &s, "STORE-BOUNDARY"));
    db.execute_batch("DROP TRIGGER injected_failure").unwrap();
    assert_eq!(evaluate(&mut c, &peer, "retained").1["value"], "43");
}
#[test]
fn output_transaction_abort_never_false_acks_or_reexecutes() {
    storage_failure("output", "unknown");
}
#[test]
fn dispatch_transaction_abort_never_executes_or_retries() {
    storage_failure("dispatch_intent", "cancelled");
}

#[test]
fn evaluator_death_quarantines_only_its_context() {
    let h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let peer = create(&mut c);
    evaluate(&mut c, &peer, "(set retained 43)");
    submit(
        &mut c,
        &submission(&s, "(erlang:exit (self) (quote kill))", "evaluator-crash"),
    );
    wait_until("evaluator death", || state(&mut c, &s) == "state_lost");
    assert_eq!(operation(&mut c, &s, "evaluator-crash")["phase"], "unknown");
    assert_eq!(evaluate(&mut c, &peer, "retained").1["value"], "43");
}

#[test]
fn host_crash_retains_history_and_unknown_work_without_reexecution() {
    let mut h = Harness::new();
    let mut c = h.client();
    let s = create(&mut c);
    let archive = create(&mut c);
    evaluate_as(&mut c, &s, "(set retained 42)", "once");
    let before = transcript(&mut c, &s);
    c.data(json!({"op":"close","session":archive["id"]}));
    wait_until("archive closed", || state(&mut c, &archive) == "closed");
    let uncertain = create(&mut c);
    let effect = h.root.path().join("once-effect");
    let source = format!(
        "(progn (file:write_file \"{}\" \"x\" (list (quote append))) (io:format \"EFFECT-COMMITTED\" ()) (timer:sleep 10000) 42)",
        effect.display()
    );
    let sub = submission(&uncertain, &source, "uncertain");
    submit(&mut c, &sub);
    wait_until("pre-crash effect", || {
        has_output(&mut c, &uncertain, "EFFECT-COMMITTED")
    });
    assert_eq!(fs::read_to_string(&effect).unwrap(), "x");
    assert_eq!(h.host.signal("-KILL").signal(), Some(9));
    drop(c);
    h.restart_host();
    let mut c = h.client();
    assert_eq!(state(&mut c, &s), "state_lost");
    assert_eq!(operation(&mut c, &s, "once")["value"], "42");
    assert!(transcript(&mut c, &s).starts_with(&before));
    assert!(h.agent.alive());
    assert_eq!(state(&mut c, &archive), "closed");
    assert_eq!(
        operation(&mut c, &uncertain, "uncertain")["phase"],
        "unknown"
    );
    assert_eq!(submit(&mut c, &sub)["phase"], "unknown");
    assert_eq!(fs::read_to_string(&effect).unwrap(), "x");
    assert!(h.host.signal("-INT").success());
}

#[test]
fn node_loss_marks_active_work_unknown_but_keeps_closed_archives() {
    let mut h = Harness::new();
    let mut c = h.client();
    let archive = create(&mut c);
    c.data(json!({"op":"close","session":archive["id"]}));
    wait_until("archive closed", || state(&mut c, &archive) == "closed");
    let sealed = transcript(&mut c, &archive);
    let s = create(&mut c);
    let sub = submission(
        &s,
        "(progn (io:format \"NODE-CRASH-START\" ()) (timer:sleep 10000) 42)",
        "agent-crash",
    );
    submit(&mut c, &sub);
    wait_until("node crash operation started", || {
        has_output(&mut c, &s, "NODE-CRASH-START")
    });
    assert_eq!(h.agent.signal("-KILL").signal(), Some(9));
    wait_until("node loss", || state(&mut c, &s) == "state_lost");
    assert_eq!(operation(&mut c, &s, "agent-crash")["phase"], "unknown");
    assert_eq!(submit(&mut c, &sub)["phase"], "unknown");
    assert_eq!(state(&mut c, &archive), "closed");
    assert_eq!(transcript(&mut c, &archive), sealed);
    assert!(h.host.signal("-INT").success());
}
