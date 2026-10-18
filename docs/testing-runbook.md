# Cayrn + LFE + xrepl testing runbook

## What you are testing

This walkthrough runs a real LFE evaluator behind Cayrn's shared Rust session
host, with two xrepl terminals attached to the same live session. It exercises
bindings, shared output, detach/reconnect, replay, operation deduplication,
interruption and explicit close. Allow about 20–30 minutes after dependencies
are installed; the automated gate can take longer on a cold build.

There are three running pieces: **the native BEAM agent**, **the Rust host**, and
**one or more Rust xrepl clients**. Rust owns admission, operation identities,
ordering and durable transcripts. The reusable `cayrn-lfe` OTP application owns
the native evaluator environments and captured I/O. Disconnecting a terminal
leaves its session alive while the host and agent remain healthy.

Checkout snapshot, verified on 2026-10-18:

| Piece | Local checkout | Branch | Commit | Role |
| --- | --- | --- | --- | --- |
| Cayrn | `/Users/oubiwann/lab/oxur/cayrn` | `main` | `53c584e` | Shared Rust crates, host, request CLI, integration tests |
| cayrn-lfe | `/Users/oubiwann/lab/lfe/cayrn-lfe` | `main` | `1b04c15` | Reusable LFE/OTP runtime agent; currently a local repository |
| xrepl | `/Users/oubiwann/lab/lfe/xrepl/xrepl` | `main` | `571def5` | Rust terminal consumer, using the local Cayrn crates |
| Cayrn planning | `/Users/oubiwann/lab/oxur/cayrn/.worktrees/planning` | `planning` | `0cd5dbf` | Project01/Arc02 plans, implementation returns and verification limits |

The runtime code is on those three `main` branches; the orphan `planning` branch
is documentation and is not merged into `main`. The separate legacy xrepl
`term` and `protocol` repositories are not needed for this prototype. No crate or
Hex publication is required: xrepl uses local Cargo path dependencies.
These commits identify the baseline, not an instruction to reset your checkouts.

The examples below use Bash and the current local directory layout. Keep four
terminal tabs available:

| Tab | Purpose |
| --- | --- |
| A | Start the agent in the background; inspect/control sessions with the CLI |
| B | Run the Rust host in the foreground |
| C | First interactive xrepl client |
| D | Second interactive xrepl client |

## 1. Build and run the automated gate

Prerequisites: Unix/macOS, Rust 1.95+, a C compiler for bundled SQLite, Erlang/OTP
27+ (initially tested with OTP 28), rebar3 and Make. Python 3 is used only for
the manual JSON extraction in step 5; the automated tests are Rust. The native project
resolves LFE 2.2.0 through rebar3. The first build may need network access for
Cargo/rebar dependencies.

```bash
cd /Users/oubiwann/lab/oxur/cayrn
make help
make check test-xrepl \
  CAYRN_LFE_DIR=/Users/oubiwann/lab/lfe/cayrn-lfe \
  XREPL_DIR=/Users/oubiwann/lab/lfe/xrepl/xrepl
```

**Pass:** Make exits zero. This builds the binaries and checks Rust formatting,
Clippy, tests and rustdoc; native evaluator tests; xrepl tests; and real
host/BEAM/client/PTY scenarios. The recorded baseline passed 16 Cayrn tests,
6 native EUnit tests, 7 xrepl tests plus 1 doctest, and 12 integration scenario
groups in the original Python runner. Its Rust replacement now has 14 individually
reported integration tests covering those scenarios. Counts may grow as the code
changes; investigate any failure rather than using the counts alone as acceptance.

The Rust integration tests create their own temporary state and processes,
including isolated host/node failure and SQLite transaction-abort tests. They do
not need your manual session below. These are local checks; they do not constitute
independent acceptance, full protocol conformance or physical power-loss testing.

To select test scope explicitly:

```bash
make test                 # Ordinary OTP-free Cayrn workspace suite
make test-integration     # Only real LFE + xrepl integration tests
make test-all             # Both suites
make test-agent           # Only host/BEAM integration; no xrepl build
```

See [integration-test commands](integration-tests.md) for individual test filters
and direct Cargo invocations. Missing prerequisites fail an opted-in run;
external-runtime targets are feature-gated out of ordinary Cargo tests.

For a quicker build-only start, use these commands instead of the full gate:

```bash
make -C /Users/oubiwann/lab/oxur/cayrn build
make -C /Users/oubiwann/lab/lfe/cayrn-lfe build
make -C /Users/oubiwann/lab/lfe/xrepl/xrepl \
  CARGO_TARGET_DIR=/Users/oubiwann/lab/lfe/xrepl/xrepl/target build-rust
```

Cayrn copies `cayrn` and `cayrn-fixture` into its `./bin`. This walkthrough uses
`./bin/cayrn` and xrepl's `target/debug/xrepl`; the fixture binary is not involved.

## 2. Start an isolated agent and host

In **A**, make a fresh private directory and start the native agent:

```bash
export CAYRN_RUN_DIR="$(mktemp -d /private/tmp/cayrn-manual.XXXXXX)"
printf 'export CAYRN_RUN_DIR=%q\n' "$CAYRN_RUN_DIR"

/Users/oubiwann/lab/lfe/cayrn-lfe/bin/cayrn-lfe \
  "$CAYRN_RUN_DIR/agent.sock" >"$CAYRN_RUN_DIR/agent.log" 2>&1 &
CAYRN_AGENT_PID=$!
printf 'Agent PID: %s\n' "$CAYRN_AGENT_PID"
```

Keep A open; it owns the PID used during cleanup. **Copy the printed `export
CAYRN_RUN_DIR=...` line into B, C and D.** A shell variable in one tab is not
shared with another tab. Do not run `mktemp` again in those tabs.

Check readiness in A:

```bash
test -S "$CAYRN_RUN_DIR/agent.sock" && echo 'Agent socket ready'
```

If startup is still in progress, repeat that check. If it never appears, inspect
`cat "$CAYRN_RUN_DIR/agent.log"` and stop here until the cause is resolved.

In **B**, after copying the export line:

```bash
/Users/oubiwann/lab/oxur/cayrn/bin/cayrn serve-agent \
  --state-dir "$CAYRN_RUN_DIR/host" \
  --agent-socket "$CAYRN_RUN_DIR/agent.sock"
```

Leave B running. **Pass:** the host prints its startup information and remains
running. Cayrn creates the private host directory and journal. Do not press
Ctrl-C in B during the ordinary session tests: it stops the host.

In **A**, define a request helper and probe the public interface:

```bash
cayrn_request() {
  /Users/oubiwann/lab/oxur/cayrn/bin/cayrn request \
    --state-dir "$CAYRN_RUN_DIR/host" "$1"
}
cayrn_request '{"op":"describe"}'
```

**Pass:** a JSON description is returned. The helper is local to A.

## 3. Attach two terminals and share a binding

In **C**:

```bash
/Users/oubiwann/lab/lfe/xrepl/xrepl/target/debug/xrepl \
  --cayrn "$CAYRN_RUN_DIR/host"
```

Copy the session ID from the startup message. In **A and D**, set it in the
shell, replacing the example value:

```bash
export SESSION_ID='paste-the-session-id-here'
```

In **D**, attach to that session:

```bash
/Users/oubiwann/lab/lfe/xrepl/xrepl/target/debug/xrepl \
  --cayrn "$CAYRN_RUN_DIR/host" --session "$SESSION_ID" --after 0
```

Enter each expression as a single line. In **C**:

```lfe
(set counter 41)
```

**Pass:** C shows a succeeded outcome with value `41`. In **D**:

```lfe
(+ counter 1)
(set counter (+ counter 1))
```

**Pass:** both return `42`. The first expression only reads; the second saves
`42`. Back in C, evaluate `counter`: it must return `42`.

Both clients receive the shared transcript, including the other client's source,
results and captured output. **While Reedline is editing, incoming events are
buffered. Press Enter on an empty line to flush peer events.** This is expected;
the prototype does not print asynchronously over an in-progress draft.

Try output and ordinary error recovery in C:

```lfe
(progn (io:format "hello ~ts~n" (list "世界")) counter)
(/ 1 0)
counter
```

**Pass:** Unicode output appears in both terminals after flushing; the divide
fails; the following `counter` still returns `42`. A user-code error should not
lose the session. `(io:get_line "input?")` is an optional negative check: program
stdin is unsupported and returns an `enotsup` error rather than consuming wire
or terminal input.

For a draft-preservation check, type `(+ counter` in C **without Enter**. In D,
evaluate `(io:format "PEER-OUTPUT~n" ())`. Finish C's draft with ` 1)` and Enter.
**Pass:** the draft remains intact, peer output flushes, and the result is `43`.
The saved counter remains `42`.

## 4. Detach, reconnect and replay without reevaluating

Type `:quit` in **both C and D**. This detaches the clients. Record each terminal's
`Detached. Resume session ... after displayed cursor N` message. The cursors can
differ because each client displays events independently.

In A:

```bash
cayrn_request "{\"op\":\"inspect_session\",\"session\":\"$SESSION_ID\"}"
```

**Pass:** `data.state` is `active` even with no terminal attached.

In D, deliberately replay the entire transcript:

```bash
/Users/oubiwann/lab/lfe/xrepl/xrepl/target/debug/xrepl \
  --cayrn "$CAYRN_RUN_DIR/host" --session "$SESSION_ID" --after 0
```

Press Enter on an empty line to display replay, then evaluate `counter`.
**Pass:** old source/results/output reappear as history, and the live value is
still `42`. Replay has not reapplied the increment.

In C, attach using **C's last displayed cursor** in place of `N`:

```bash
/Users/oubiwann/lab/lfe/xrepl/xrepl/target/debug/xrepl \
  --cayrn "$CAYRN_RUN_DIR/host" --session 'paste-the-session-id-here' --after N
```

`--after` means the last event already displayed, not the next wanted event.
Only later events should replay. Cursor persistence across terminal crashes is
not automatic yet; save the displayed cursor or intentionally replay from zero.

## 5. Verify an explicit retry executes only once

Leave the clients attached. In **A**, obtain the generation and construct one
stable submission. This test depends on the saved counter still being `42`.

```bash
SESSION_JSON="$(cayrn_request "{\"op\":\"inspect_session\",\"session\":\"$SESSION_ID\"}")"
GENERATION_ID="$(printf '%s' "$SESSION_JSON" | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["data"]["generation"])')"
SUBMISSION="$(printf '{"op":"submit","submission":{"session":"%s","generation":"%s","operation":"runbook-once","client":"runbook-cli","source":"(set counter (+ counter 1))"}}' \
  "$SESSION_ID" "$GENERATION_ID")"
cayrn_request "$SUBMISSION"
cayrn_request "$SUBMISSION"
```

Both responses refer to the same operation. Submission returns admission or the
existing record; it does not wait for evaluation completion. Inspect it:

```bash
cayrn_request "{\"op\":\"inspect_operation\",\"session\":\"$SESSION_ID\",\"operation\":\"runbook-once\"}"
```

If the phase is `queued` or `dispatched`, repeat the inspection until terminal.
**Pass:** phase `succeeded`, value `43`; evaluating `counter` from C or D returns
`43`, not `44`. A matching retry has returned the original record.

A new evaluation needs a new operation ID. Reusing `runbook-once` with different
source, client or generation should fail with a conflict. Clients never retry
an uncertain evaluation automatically. To repeat this test on the same session,
choose a new ID and adjust the expected starting/result values.

## 6. Check late output attribution

In C:

```lfe
(progn (spawn (lambda () (timer:sleep 500) (io:format "LATE-ORIGIN~n" ()))) (quote scheduled))
```

After the `scheduled` result, evaluate `counter` before or after the late write.
Press Enter in both clients to flush buffered output.

**Pass:** `LATE-ORIGIN` appears with the operation ID of the `scheduled` expression,
even if a later evaluation has already run. It must not be attributed to the
`counter` request. Output captured after its originating evaluation returns is
still part of the durable transcript.

## 7. Interrupt a disposable session; preserve its peer

Use a **new session** for this test: quit C, then start C again **without
`--session`**:

```bash
/Users/oubiwann/lab/lfe/xrepl/xrepl/target/debug/xrepl \
  --cayrn "$CAYRN_RUN_DIR/host"
```

Keep D attached to the original session. Record C's new session ID in A as
`INTERRUPT_SESSION_ID` if you want to inspect it. In C:

```lfe
(progn (io:format "INTERRUPT-START~n" ()) (timer:sleep 10000) 99)
```

After `INTERRUPT-START`, press **Ctrl-C in C while the evaluation is running**.
**Pass:** the client reports `interrupt_requested_session_destructive`; the
operation eventually reports cancelled and the session reports state loss.
The receipt alone is not confirmation. A completed operation can win the race,
so interrupt well before the ten seconds elapse.

The LFE capability currently discards the interrupted session's environment.
Create a fresh session for further evaluation there. In **D**, `counter` must
still return `43`: interrupting one context must not kill the BEAM or its peers.
Ctrl-C while merely editing a draft clears the line and does not interrupt a
runtime evaluation.

## 8. Close explicitly and retain a read-only transcript

In **D**, type `:close`. The client reports that close was requested and detaches.
In **A**, inspect the **original** session:

```bash
cayrn_request "{\"op\":\"inspect_session\",\"session\":\"$SESSION_ID\"}"
```

Repeat if it is still `closing`. **Pass:** it reaches `closed`. Close completion
requires the native capture barrier; a request receipt is insufficient. A lost
link or barrier timeout can leave an incomplete `closing` session and should be
investigated rather than treated as a completed archive.

Read the transcript:

```bash
cayrn_request "{\"op\":\"read\",\"session\":\"$SESSION_ID\",\"after\":\"0\",\"limit\":64}"
```

Continue with `data.next` as the quoted `after` string until it equals
`data.head`. **Pass:** history remains readable and the final event is
`session_closed`. Repeating a read from the same cursor returns the same page;
no later capture events should appear beyond the final close event.

Try a **new** submission after close:

```bash
CLOSED_SUBMISSION="$(printf '{"op":"submit","submission":{"session":"%s","generation":"%s","operation":"runbook-after-close","client":"runbook-cli","source":"counter"}}' \
  "$SESSION_ID" "$GENERATION_ID")"
cayrn_request "$CLOSED_SUBMISSION"
```

**Pass:** the CLI exits nonzero with an error; no evaluation is admitted. Retrying
the old matching `runbook-once` submission would instead return its existing
record, so it is not a valid test for rejecting new work. A repeated close is
idempotent:

```bash
cayrn_request "{\"op\":\"close\",\"session\":\"$SESSION_ID\"}"
```

## 9. Optional host-restart check

Use this after the other tests. Create a fresh session in C, record its ID, and
save `(set restart-probe 17)`. In A, set
`RESTART_SESSION_ID='paste-the-new-session-id-here'`. Quit C.
Press Ctrl-C in **B** to stop the host,
then restart it with the identical `serve-agent` command and state directory.
Keep the native agent in A running throughout.

In A, inspect the saved session and read its history:

```bash
cayrn_request "{\"op\":\"inspect_session\",\"session\":\"$RESTART_SESSION_ID\"}"
cayrn_request "{\"op\":\"read\",\"session\":\"$RESTART_SESSION_ID\",\"after\":\"0\",\"limit\":64}"
```

**Pass:** the old session's committed transcript is still readable, but its state
is `state_lost`; its old heap/bindings are not restored. New evaluations require
a new session. The surviving BEAM does not make the old host-agent context
recoverable. Closed archives from step 8 remain closed and readable.

The automated gate additionally kills the host abruptly while BEAM stays alive
and verifies uncertain in-flight work is not silently rerun. Use that isolated
scenario for crash testing rather than killing unrelated local BEAM processes.

## 10. Stop the test processes

Quit any remaining clients. Stop the foreground host in **B** with Ctrl-C.
Then in **A**, stop only the native process this run started:

```bash
kill -TERM "$CAYRN_AGENT_PID"
wait "$CAYRN_AGENT_PID"
printf 'Retained test state and logs: %s\n' "$CAYRN_RUN_DIR"
```

Keep the directory if you want to inspect the SQLite journal or agent log. To
remove this run's disposable files later, use this guarded command in A:

```bash
case "$CAYRN_RUN_DIR" in
  /private/tmp/cayrn-manual.?*) rm -r -- "$CAYRN_RUN_DIR" ;;
  *) printf 'Unexpected test directory; leaving it untouched\n' >&2 ;;
esac
```

## Troubleshooting and interpretation

| Symptom | Check / expected behavior |
| --- | --- |
| Host cannot connect | Confirm the same exported run directory in every tab, agent socket readiness and `agent.log`. |
| Existing agent socket / address in use | Choose a fresh test directory; do not unlink another process's endpoint. |
| Host rejects directory permissions | Use a new directory or a deliberately private mode-0700 state directory. |
| No peer output while typing | Press Enter on an empty line; the presentation spool buffers while editing. |
| xrepl starts the IF mock | Include `--cayrn "$CAYRN_RUN_DIR/host"`. |
| Cargo local dependency cannot be found | Preserve the checkout layout in the introductory table, or update the path dependencies deliberately. |
| `queued` response without a result | Inspect the operation or follow/read the transcript; admission and completion are separate. |
| Session lost after interruption/restart | Expected for this LFE profile; start a fresh session. |
| Presentation spool fills | Reattach from the last displayed cursor. The authoritative host transcript is retained. |

The native profile captures inherited group-leader I/O, including late writers.
Explicit/global devices (including global stderr), unmanaged processes and
external side effects are outside that boundary. These tests run trusted code
in a dedicated node. Current budgets include 256 operations per native context,
8192-byte source/output/value previews and an 8 MiB client presentation spool.
The full transcript belongs to the host journal, not that bounded client spool.

Two terminals demonstrate simultaneous attachments; an actual editor plugin,
old xrepl session migration, heap restoration and package publication remain
future work. See [the native-agent profile](native-agent.md) for the contract and
capability limits, and the planning checkout's
`project01-shared-repl-architecture/arc02-native-agent-integration/closing-report.md`
for the implementation return and pending independent acceptance.

## Runbook verification

On 2026-10-18, all Bash blocks passed syntax checks and the relative Markdown
links resolved. A fresh real agent/host pair exercised the documented LFE
examples, Unicode output, ordinary error recovery, late output attribution,
CLI operation deduplication, close and post-close rejection, repeated close,
and host restart with a surviving BEAM. Those checks passed and their temporary
processes/state were cleaned up. Interactive keyboard behavior is covered by the
baseline PTY integration tests and the manual steps above. The initial
runbook-only edit did not rerun the full gate. The later Rust test-runner port
did; see [its verification record](integration-tests.md#verification).
