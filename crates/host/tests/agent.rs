//! The connector is tested against a non-language protocol peer as well as BEAM.
use cayrn_client::Client;
use cayrn_host::Host;
use cayrn_protocol::{Cursor, EventKind, Phase, Request, Response, Submission};
use serde_json::{Value, json};
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};

fn read(stream: &mut std::os::unix::net::UnixStream) -> Value {
    let mut size = [0; 4];
    stream.read_exact(&mut size).unwrap();
    let mut bytes = vec![0; u32::from_be_bytes(size) as usize];
    stream.read_exact(&mut bytes).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
fn write(stream: &mut std::os::unix::net::UnixStream, value: Value) {
    let bytes = serde_json::to_vec(&value).unwrap();
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .unwrap();
    stream.write_all(&bytes).unwrap();
}
#[tokio::test]
async fn generic_connector_captures_acknowledged_non_lfe_peer_output() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("agent.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let hello = read(&mut stream);
        assert_eq!(hello["kind"], "hello");
        write(
            &mut stream,
            json!({"kind":"ready","version":1,"backend":"non-language-double"}),
        );
        let dispatch = read(&mut stream);
        assert_eq!(dispatch["source"], "arbitrary text");
        assert_eq!(dispatch["generation"], hello["generation"]);
        assert_eq!(dispatch["authority"], hello["authority"]);
        write(
            &mut stream,
            json!({"kind":"output","operation":dispatch["operation"],"position":"1","text":"output from independent peer"}),
        );
        let ack = read(&mut stream);
        assert_eq!(ack, json!({"kind":"ack","position":"1"}));
        write(
            &mut stream,
            json!({"kind":"outcome","operation":dispatch["operation"],"phase":"succeeded","value":"independent value"}),
        );
        let mut b = [0];
        let _ = stream.read(&mut b); // Host owns orderly connection shutdown.
    });
    let host = Host::bind_agent(root.path().join("state"), socket)
        .await
        .unwrap();
    let mut client = Client::connect(host.socket_path()).await.unwrap();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(host.serve(async {
        let _ = rx.await;
    }));
    let Response::Session(s) = client.request(Request::Create).await.unwrap() else {
        panic!()
    };
    let sub = Submission {
        session: s.id.clone(),
        generation: s.generation,
        operation: "op".into(),
        client: "test".into(),
        source: "arbitrary text".into(),
    };
    client
        .request(Request::Submit { submission: sub })
        .await
        .unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let Response::Operation(op) = client
                .request(Request::InspectOperation {
                    session: s.id.clone(),
                    operation: "op".into(),
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            if op.phase == Phase::Succeeded {
                assert_eq!(op.value.as_deref(), Some("independent value"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let Response::Events(page) = client
        .request(Request::Read {
            session: s.id,
            after: Cursor(0),
            limit: 64,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert!(page.events.iter().any(
        |e| matches!(&e.event,EventKind::Output {text,..} if text=="output from independent peer")
    ));
    stop.send(()).unwrap();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    peer.join().unwrap();
}

#[tokio::test]
async fn wrong_agent_version_cannot_create_a_usable_context() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("agent.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read(&mut stream);
        write(
            &mut stream,
            json!({"kind":"ready","version":999,"backend":"wrong"}),
        );
    });
    let host = Host::bind_agent(root.path().join("state"), socket)
        .await
        .unwrap();
    let mut client = Client::connect(host.socket_path()).await.unwrap();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(host.serve(async {
        let _ = rx.await;
    }));
    let error = client.request(Request::Create).await.unwrap_err();
    assert_eq!(error.code, cayrn_protocol::ErrorCode::RuntimeLost);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    peer.join().unwrap();
}

#[tokio::test]
async fn link_loss_during_close_never_seals_without_native_barrier() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("agent.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read(&mut stream);
        write(
            &mut stream,
            json!({"kind":"ready","version":1,"backend":"incomplete-close-double","close_barrier":true}),
        );
        assert_eq!(read(&mut stream)["kind"], "close");
        // Socket EOF is deliberately substituted for a barrier; it must not count.
    });
    let host = Host::bind_agent(root.path().join("state"), socket)
        .await
        .unwrap();
    let mut client = Client::connect(host.socket_path()).await.unwrap();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(host.serve(async {
        let _ = rx.await;
    }));
    let Response::Session(session) = client.request(Request::Create).await.unwrap() else {
        panic!()
    };
    client
        .request(Request::Close {
            session: session.id.clone(),
        })
        .await
        .unwrap();
    let page = timeout(Duration::from_secs(3), async {
        loop {
            let Response::Events(page) = client
                .request(Request::Read {
                    session: session.id.clone(),
                    after: Cursor(0),
                    limit: 64,
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            if page
                .events
                .iter()
                .any(|e| matches!(e.event, EventKind::StateLost { .. }))
            {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !page
            .events
            .iter()
            .any(|e| matches!(e.event, EventKind::SessionClosed))
    );
    let Response::Session(state) = client
        .request(Request::InspectSession {
            session: session.id,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(state.state, cayrn_protocol::SessionState::Closing);
    stop.send(()).unwrap();
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    peer.join().unwrap();
}

#[tokio::test]
async fn cooperative_agent_interrupt_preserves_context_without_language_assumptions() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("agent.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let (started, ready) = oneshot::channel();
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read(&mut stream);
        write(
            &mut stream,
            json!({"kind":"ready","version":1,"backend":"cooperative-double","interrupt":"cooperative"}),
        );
        let first = read(&mut stream);
        started.send(()).unwrap();
        assert_eq!(
            read(&mut stream),
            json!({"kind":"cancel","operation":first["operation"]})
        );
        write(
            &mut stream,
            json!({"kind":"outcome","operation":first["operation"],"phase":"cancelled","value":"cooperatively interrupted"}),
        );
        let second = read(&mut stream);
        write(
            &mut stream,
            json!({"kind":"outcome","operation":second["operation"],"phase":"succeeded","value":"context retained"}),
        );
        let mut b = [0];
        let _ = stream.read(&mut b);
    });
    let host = Host::bind_agent(root.path().join("state"), socket)
        .await
        .unwrap();
    let mut client = Client::connect(host.socket_path()).await.unwrap();
    let (stop, rx) = oneshot::channel();
    let task = tokio::spawn(host.serve(async {
        let _ = rx.await;
    }));
    let Response::Session(session) = client.request(Request::Create).await.unwrap() else {
        panic!()
    };
    let make = |id: &str| Submission {
        session: session.id.clone(),
        generation: session.generation.clone(),
        operation: id.into(),
        client: "test".into(),
        source: id.into(),
    };
    client
        .request(Request::Submit {
            submission: make("wait"),
        })
        .await
        .unwrap();
    ready.await.unwrap();
    assert_eq!(
        client
            .request(Request::Cancel {
                session: session.id.clone(),
                operation: "wait".into()
            })
            .await
            .unwrap(),
        Response::Control {
            disposition: "interrupt_requested_cooperative".into()
        }
    );
    timeout(Duration::from_secs(3), async {
        loop {
            let Response::Operation(op) = client
                .request(Request::InspectOperation {
                    session: session.id.clone(),
                    operation: "wait".into(),
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            if op.phase == Phase::Cancelled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    client
        .request(Request::Submit {
            submission: make("probe"),
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let Response::Operation(op) = client
                .request(Request::InspectOperation {
                    session: session.id.clone(),
                    operation: "probe".into(),
                })
                .await
                .unwrap()
            else {
                panic!()
            };
            if op.phase == Phase::Succeeded {
                assert_eq!(op.value.as_deref(), Some("context retained"));
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let Response::Session(state) = client
        .request(Request::InspectSession {
            session: session.id,
        })
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(state.state, cayrn_protocol::SessionState::Active);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    peer.join().unwrap();
}
