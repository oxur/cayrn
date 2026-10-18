use cayrn_client::Client;
use cayrn_host::Host;
use cayrn_protocol::{
    Call, Cursor, ErrorCode, Event, EventKind, Operation, Phase, Request, Response, Session,
    SessionState, Submission, VERSION, wire,
};
use std::{os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};
use tempfile::TempDir;
use tokio::{
    io::AsyncWriteExt,
    net::UnixStream,
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};

struct Rig {
    directory: TempDir,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<cayrn_protocol::Result<()>>>,
}
impl Rig {
    async fn start() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("cyrn-")
            .tempdir_in("/private/tmp")
            .unwrap_or_else(|_| tempfile::tempdir().unwrap());
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut rig = Self {
            directory,
            stop: None,
            task: None,
        };
        rig.restart().await;
        rig
    }
    async fn restart(&mut self) {
        let host = Host::bind(self.directory.path(), env!("CARGO_BIN_EXE_cayrn-fixture"))
            .await
            .unwrap();
        let (tx, rx) = oneshot::channel();
        self.stop = Some(tx);
        self.task = Some(tokio::spawn(host.serve(async {
            let _ = rx.await;
        })));
    }
    fn socket(&self) -> PathBuf {
        self.directory.path().join("cayrn.sock")
    }
    async fn client(&self) -> Client {
        Client::connect(self.socket()).await.unwrap()
    }
    async fn shutdown(&mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for Rig {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
async fn create(client: &mut Client) -> Session {
    match client.request(Request::Create).await.unwrap() {
        Response::Session(s) => s,
        _ => panic!("expected session"),
    }
}
fn submission(session: &Session, id: &str, client: &str, source: &str) -> Submission {
    Submission {
        session: session.id.clone(),
        generation: session.generation.clone(),
        operation: id.into(),
        client: client.into(),
        source: source.into(),
    }
}
async fn submit(client: &mut Client, sub: &Submission) -> Operation {
    match client
        .request(Request::Submit {
            submission: sub.clone(),
        })
        .await
        .unwrap()
    {
        Response::Operation(o) => o,
        _ => panic!("expected operation"),
    }
}
async fn inspect(client: &mut Client, sub: &Submission) -> Operation {
    match client
        .request(Request::InspectOperation {
            session: sub.session.clone(),
            operation: sub.operation.clone(),
        })
        .await
        .unwrap()
    {
        Response::Operation(o) => o,
        _ => panic!("expected operation"),
    }
}
async fn finished(client: &mut Client, sub: &Submission) -> Operation {
    timeout(Duration::from_secs(4), async {
        loop {
            let op = inspect(client, sub).await;
            if !matches!(op.phase, Phase::Queued | Phase::Dispatched) {
                break op;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
async fn events(client: &mut Client, sid: &str) -> Vec<Event> {
    let mut after = Cursor(0);
    let mut result = Vec::new();
    loop {
        let Response::Events(page) = client
            .request(Request::Read {
                session: sid.into(),
                after,
                limit: 3,
            })
            .await
            .unwrap()
        else {
            panic!("expected page")
        };
        assert!(page.events.len() <= 3);
        for event in &page.events {
            assert_eq!(event.sequence.0, after.0 + 1);
            after = event.sequence;
        }
        assert_eq!(page.next, after);
        result.extend(page.events);
        if after == page.head {
            break;
        }
    }
    result
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_share_ordered_state_and_replay() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let mut b = rig.client().await;
    let sa = session.clone();
    let sb = session.clone();
    let fa = async {
        for i in 0..10 {
            submit(&mut a, &submission(&sa, &format!("a-{i}"), "a", "add 1")).await;
        }
    };
    let fb = async {
        for i in 0..10 {
            submit(&mut b, &submission(&sb, &format!("b-{i}"), "b", "add 1")).await;
        }
    };
    tokio::join!(fa, fb);
    assert_eq!(
        finished(&mut a, &submission(&session, "a-9", "a", "add 1"))
            .await
            .phase,
        Phase::Succeeded
    );
    assert_eq!(
        finished(&mut b, &submission(&session, "b-9", "b", "add 1"))
            .await
            .phase,
        Phase::Succeeded
    );
    let ea = events(&mut a, &session.id).await;
    let eb = events(&mut b, &session.id).await;
    assert_eq!(ea, eb);
    let values = ea
        .iter()
        .filter_map(|e| match &e.event {
            EventKind::Outcome {
                phase: Phase::Succeeded,
                value: Some(v),
                ..
            } => Some(v.parse::<u64>().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(values, (1..=20).collect::<Vec<_>>());
    let accepted = ea
        .iter()
        .filter_map(|e| {
            if let EventKind::Accepted { submission } = &e.event {
                Some(submission.operation.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(accepted.len(), 20);
    let outcomes = ea
        .iter()
        .filter_map(|e| {
            if let EventKind::Outcome { operation, .. } = &e.event {
                Some(operation.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(accepted, outcomes);
    drop(a);
    drop(b);
    rig.shutdown().await;
}
#[tokio::test]
async fn last_client_detach_keeps_live_state_and_unicode_output() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let add = submission(&session, "set", "terminal", "add 7");
    submit(&mut a, &add).await;
    assert_eq!(finished(&mut a, &add).await.value.as_deref(), Some("7"));
    drop(a);
    let mut editor = rig.client().await;
    let get = submission(&session, "read", "editor", "get");
    submit(&mut editor, &get).await;
    assert_eq!(
        finished(&mut editor, &get).await.value.as_deref(),
        Some("7")
    );
    let echo = submission(&session, "unicode", "editor", "echo λ — 🪨");
    submit(&mut editor, &echo).await;
    finished(&mut editor, &echo).await;
    assert!(events(&mut editor,&session.id).await.iter().any(|e|matches!(&e.event,EventKind::Output {operation,text} if operation=="unicode" && text=="λ — 🪨")));
    drop(editor);
    rig.shutdown().await;
}
#[tokio::test]
async fn lost_reply_duplicate_and_conflicting_id_never_repeat_mutation() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let sub = submission(&session, "once", "terminal", "add 5");
    let mut lost = UnixStream::connect(rig.socket()).await.unwrap();
    wire::write(
        &mut lost,
        &Call {
            version: VERSION,
            id: "lost-reply".into(),
            request: Request::Submit {
                submission: sub.clone(),
            },
        },
    )
    .await
    .unwrap();
    drop(lost);
    submit(&mut a, &sub).await;
    assert_eq!(finished(&mut a, &sub).await.value.as_deref(), Some("5"));
    submit(&mut a, &sub).await;
    let changed = submission(&session, "once", "terminal", "add 99");
    assert_eq!(
        a.request(Request::Submit {
            submission: changed
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::Conflict
    );
    let get = submission(&session, "verify", "terminal", "get");
    submit(&mut a, &get).await;
    assert_eq!(finished(&mut a, &get).await.value.as_deref(), Some("5"));
    assert_eq!(events(&mut a,&session.id).await.iter().filter(|e|matches!(&e.event,EventKind::Accepted {submission} if submission.operation=="once")).count(),1);
    drop(a);
    rig.shutdown().await;
}
#[tokio::test]
async fn host_restart_retains_history_but_never_restores_heap_by_reexecution() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let sub = submission(&session, "saved", "terminal", "add 42");
    submit(&mut a, &sub).await;
    let original = finished(&mut a, &sub).await;
    let before = events(&mut a, &session.id).await;
    drop(a);
    rig.shutdown().await;
    rig.restart().await;
    let mut b = rig.client().await;
    let Response::Session(old) = b
        .request(Request::InspectSession {
            session: session.id.clone(),
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(old.state, SessionState::StateLost);
    assert_eq!(old.generation, session.generation);
    assert_eq!(submit(&mut b, &sub).await, original);
    let fresh = submission(&session, "no-replay", "terminal", "get");
    assert_eq!(
        b.request(Request::Submit { submission: fresh })
            .await
            .unwrap_err()
            .code,
        ErrorCode::StateLost
    );
    let after = events(&mut b, &session.id).await;
    assert_eq!(&after[..before.len()], &before);
    assert!(matches!(
        after.last().unwrap().event,
        EventKind::StateLost { .. }
    ));
    let new_session = create(&mut b).await;
    assert_ne!(new_session.generation, session.generation);
    let get = submission(&new_session, "fresh", "terminal", "get");
    submit(&mut b, &get).await;
    assert_eq!(finished(&mut b, &get).await.value.as_deref(), Some("0"));
    drop(b);
    rig.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_worker_does_not_block_inspection_or_another_session() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let slow = submission(&session, "slow", "a", "sleep 1000");
    submit(&mut a, &slow).await;
    timeout(Duration::from_secs(3), async {
        loop {
            if inspect(&mut a, &slow).await.phase == Phase::Dispatched {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut b = rig.client().await;
    assert!(matches!(
        b.request(Request::Describe).await.unwrap(),
        Response::Description { .. }
    ));
    let other = create(&mut b).await;
    let add = submission(&other, "fast", "b", "add 3");
    submit(&mut b, &add).await;
    assert_eq!(finished(&mut b, &add).await.value.as_deref(), Some("3"));
    assert_eq!(inspect(&mut a, &slow).await.phase, Phase::Dispatched);
    finished(&mut a, &slow).await;
    drop(a);
    drop(b);
    rig.shutdown().await;
}
#[tokio::test]
async fn worker_death_is_unknown_and_does_not_affect_peer_session() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let other = create(&mut a).await;
    let crash = submission(&session, "crash", "a", "crash");
    submit(&mut a, &crash).await;
    assert_eq!(finished(&mut a, &crash).await.phase, Phase::Unknown);
    let next = submission(&session, "next", "a", "get");
    assert_eq!(
        a.request(Request::Submit { submission: next })
            .await
            .unwrap_err()
            .code,
        ErrorCode::StateLost
    );
    let peer = submission(&other, "peer", "a", "add 2");
    submit(&mut a, &peer).await;
    assert_eq!(finished(&mut a, &peer).await.value.as_deref(), Some("2"));
    drop(a);
    rig.shutdown().await;
}
#[tokio::test]
async fn exclusive_owner_and_boundaries_reject_invalid_work() {
    let mut rig = Rig::start().await;
    let duplicate = Host::bind(rig.directory.path(), env!("CARGO_BIN_EXE_cayrn-fixture"))
        .await
        .unwrap_err();
    assert_eq!(duplicate.code, ErrorCode::Busy);
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let mut sub = submission(&session, "wrong", "a", "add 1");
    sub.generation = "wrong-generation".into();
    assert_eq!(
        a.request(Request::Submit { submission: sub })
            .await
            .unwrap_err()
            .code,
        ErrorCode::StaleGeneration
    );
    let too_big = submission(
        &session,
        "large",
        "a",
        &"x".repeat(cayrn_protocol::MAX_SOURCE + 1),
    );
    assert_eq!(
        a.request(Request::Submit {
            submission: too_big
        })
        .await
        .unwrap_err()
        .code,
        ErrorCode::InvalidRequest
    );
    for (after, limit) in [(Cursor(u64::MAX), 2), (Cursor(0), 0), (Cursor(0), 65)] {
        assert_eq!(
            a.request(Request::Read {
                session: session.id.clone(),
                after,
                limit
            })
            .await
            .unwrap_err()
            .code,
            ErrorCode::InvalidRequest
        );
    }
    // Bypass the SDK's early validation and prove the host boundary too.
    let mut raw = UnixStream::connect(rig.socket()).await.unwrap();
    let too_big = submission(
        &session,
        "raw-large",
        "a",
        &"x".repeat(cayrn_protocol::MAX_SOURCE + 1),
    );
    wire::write(
        &mut raw,
        &Call {
            version: VERSION,
            id: "raw-large".into(),
            request: Request::Submit {
                submission: too_big,
            },
        },
    )
    .await
    .unwrap();
    let reply: cayrn_protocol::Reply = wire::read(&mut raw).await.unwrap();
    assert!(matches!(reply.response,Response::Error(e) if e.code==ErrorCode::InvalidRequest));
    drop(raw);
    let before = events(&mut a, &session.id).await;
    assert_eq!(before.len(), 1);
    let mut wire_client = UnixStream::connect(rig.socket()).await.unwrap();
    wire::write(
        &mut wire_client,
        &Call {
            version: 999,
            id: "version".into(),
            request: Request::Create,
        },
    )
    .await
    .unwrap();
    let reply: cayrn_protocol::Reply = wire::read(&mut wire_client).await.unwrap();
    assert!(matches!(reply.response,Response::Error(e) if e.code==ErrorCode::Protocol));
    for bytes in [
        (cayrn_protocol::MAX_FRAME as u32 + 1)
            .to_be_bytes()
            .to_vec(),
        [1_u32.to_be_bytes().as_slice(), b"{"].concat(),
    ] {
        let mut malformed = UnixStream::connect(rig.socket()).await.unwrap();
        malformed.write_all(&bytes).await.unwrap();
        let result = timeout(
            Duration::from_secs(2),
            wire::read::<_, cayrn_protocol::Reply>(&mut malformed),
        )
        .await
        .unwrap();
        assert!(result.is_err());
    }
    assert_eq!(events(&mut a, &session.id).await, before);
    drop(a);
    drop(wire_client);
    rig.shutdown().await;
}

#[tokio::test]
async fn user_code_error_preserves_context_and_is_not_runtime_failure() {
    let mut rig = Rig::start().await;
    let mut a = rig.client().await;
    let session = create(&mut a).await;
    let full = submission(&session, "full", "a", "add 9223372036854775807");
    submit(&mut a, &full).await;
    assert_eq!(finished(&mut a, &full).await.phase, Phase::Succeeded);
    let overflow = submission(&session, "overflow", "a", "add 1");
    submit(&mut a, &overflow).await;
    assert_eq!(finished(&mut a, &overflow).await.phase, Phase::Failed);
    let get = submission(&session, "retained", "a", "get");
    submit(&mut a, &get).await;
    assert_eq!(
        finished(&mut a, &get).await.value.as_deref(),
        Some("9223372036854775807")
    );
    drop(a);
    rig.shutdown().await;
}
#[tokio::test]
async fn insecure_directory_is_rejected_without_changing_its_permissions() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = Host::bind(directory.path(), env!("CARGO_BIN_EXE_cayrn-fixture"))
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::InvalidRequest);
    assert_eq!(
        std::fs::metadata(directory.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert!(!directory.path().join("journal.sqlite3").exists());
}
