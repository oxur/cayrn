# Native-agent prototype

Cayrn's shared Rust connector now supports the thin `cayrn-lfe` OTP application.
It is an experimental real-language integration alongside the fixture backend.
Other native agents can implement the same internal contract; no language-specific
Rust crate or language switch is built into the connector.

Build the sibling `../../lfe/cayrn-lfe` project with `make check` (OTP 27+, rebar3,
LFE 2.2.0). Start its `bin/cayrn-lfe` launcher with a socket in a fresh private
state directory, then:

```sh
make build
./bin/cayrn serve-agent --state-dir /tmp/cayrn-native-host \
  --agent-socket /private/path/agent.sock
./bin/cayrn request --state-dir /tmp/cayrn-native-host '{"op":"create"}'
```

Use the returned session ID/generation with ordinary submit requests, using
`(set x 41)` and `(+ x 1)` as source. Poll operation.inspect and read events as
in the main README. Clients disconnect independently of the agent connection.
The host remains the sole admission/journal authority. The native project has
no dependency on xrepl; xrepl is the first intended terminal consumer.

`make test-agent CAYRN_LFE_DIR=/path/to/cayrn-lfe` runs real native tests plus
host/BEAM integration. Ordinary `make check` still runs without OTP and retains
the fixture regression cases and non-LFE connector contract double.

The agent contract is separately versioned from public client RPC. It uses the
same four-byte bounded JSON framing mechanics, not the line-based fixture wire.
A context handshake carries session, generation and authority. The connector
keeps an independent reader, bounded event handling and a wakeable control socket.
Captured writes receive an ACK only after the Rust SQLite transaction commits.
Unknown/discontinuous capture or a malformed agent reply loses that context;
there is no automatic native reconnect, dispatch retry or code replay.

Initially tested on OTP 28. The native profile limits contexts to 256 operations
and retains immutable operation attribution for late writers. Supported I/O is
through the evaluator's inherited group leader; explicit/global devices and
unmanaged external effects are outside that capture/resource boundary. Unsupported
stdin returns an error. Trusted code may affect the shared node. No sandbox,
heap restore, complete arbitrary-output capture or production-readiness claim.
See the native project's README for its node/context budgets and restart policy.

## xrepl first consumer

The sibling xrepl Rust terminal now consumes these shared crates through local
workspace dependencies (Rust 1.95+):

```sh
make -C ../../lfe/xrepl/xrepl build-rust
../../lfe/xrepl/xrepl/target/debug/xrepl --cayrn /tmp/cayrn-native-host
# Reattach without recreating the live environment:
../../lfe/xrepl/xrepl/target/debug/xrepl --cayrn /tmp/cayrn-native-host \
  --session SESSION_ID --after LAST_DISPLAYED_CURSOR
```

The terminal reports its session and displayed resume cursor. Its separate event
connection follows the complete journal by exact cursor, storing pages in a
bounded 8MiB private temporary spool. No replay/live subscription switch can lose
an event. While Reedline edits, output is buffered and flushed on the next input
or EOF; incoming events never overwrite draft text. Foreground results flush
before the next prompt. Spool exhaustion ends attachment and leaves the host
transcript intact; reconnect from the last actually displayed cursor. Automatic
persistent terminal cursors across process crashes are not yet implemented.

`--cayrn-json` accepts source lines and emits session metadata/full JSON events
for tools and automated tests. The ordinary default IF mock mode remains intact.
`make test-xrepl` runs Rust consumer gates and real BEAM/host/headless/PTY cases.
A headless second client is not a delivered editor plugin. Global terminal UX
and richer language services remain separate work.
