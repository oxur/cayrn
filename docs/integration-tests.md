# Rust integration tests

The former `scripts/test-native-agent.py` scenarios now live in two Cargo
integration-test targets in `cayrn-cli`. There is no Python test runner.
Ordinary workspace tests still include the fast fixture/socket and non-LFE
contract tests; the optional targets below require real external runtimes.

## Choose the scope

Run these from the Cayrn checkout:

| Command | Runs | External prerequisites |
| --- | --- | --- |
| `make test` | Ordinary workspace test targets | Rust + C compiler; no OTP or xrepl |
| `make test-agent` | 11 real host/BEAM tests | Built automatically: cayrn-lfe; OTP 27+, rebar3 |
| `make test-integration` | All 14 real-runtime tests: 11 native + 3 consumer | Above, plus the xrepl checkout (built automatically) |
| `make test-all` | Ordinary workspace suite + all 14 integration tests | Both runtime checkouts |
| `make check test-xrepl` | Existing full quality gate, including native EUnit and xrepl unit/doctests | Both runtime checkouts |
| `make lint-integration` | Compile/Clippy all optional Rust test code | No running OTP, native build or xrepl checkout needed |

`make check` remains the OTP-free CI gate. No tests use `#[ignore]` or return
success because a dependency is absent. Cargo excludes the external-runtime
targets unless their required features are enabled; an explicitly enabled run
fails with a prerequisite diagnostic if the agent or consumer is missing.

Default checkout paths match [the manual runbook](testing-runbook.md). Override
Make's paths when needed:

```bash
make test-integration \
  CAYRN_LFE_DIR=/absolute/path/to/cayrn-lfe \
  XREPL_DIR=/absolute/path/to/xrepl
```

Make preserves Cayrn's `./bin` copies and builds the xrepl consumer in its own
`target` directory. The Rust tests themselves launch Cargo's selected Cayrn
binary (`CARGO_BIN_EXE_cayrn`), avoiding an accidentally stale `./bin` host.
Make serializes scenario execution by default to keep resource usage modest;
each test has isolated state and can also run under Cargo's parallel harness.

## Cargo features and individual tests

`native-integration` enables the `native_agent` test target.
`xrepl-integration` includes it and enables the `xrepl` target and its optional
`portable-pty` dependency. The features do not alter production behavior.

After building the native agent and, for consumer tests, xrepl:

```bash
# Only native integration tests; no ordinary workspace tests.
CAYRN_LFE_DIR=/Users/oubiwann/lab/lfe/cayrn-lfe \
  cargo test -p cayrn-cli --features native-integration \
  --test native_agent --locked -- --test-threads=1

# Only the three terminal/headless consumer integration tests.
CAYRN_LFE_DIR=/Users/oubiwann/lab/lfe/cayrn-lfe \
XREPL_BIN=/Users/oubiwann/lab/lfe/xrepl/xrepl/target/debug/xrepl \
  cargo test -p cayrn-cli --features xrepl-integration \
  --test xrepl --locked -- --test-threads=1

# All normal workspace targets plus the optional runtime targets.
# Build the sibling native/consumer projects first; Cargo doesn't build them.
cargo test --workspace --all-targets --features cayrn-cli/xrepl-integration \
  --locked -- --test-threads=1

# List the native cases without launching a runtime.
cargo test -p cayrn-cli --features native-integration \
  --test native_agent --locked -- --list

# One failure scenario, via Make's test-harness arguments.
make test-agent TEST_ARGS='host_crash --nocapture'

# One actual keyboard test, via Cargo's name filter.
cargo test -p cayrn-cli --features xrepl-integration --test xrepl \
  keyboard_ctrl_c --locked -- --nocapture
```

Direct Cargo commands use `CAYRN_LFE_DIR` and `XREPL_BIN` environment variables,
with the documented sibling paths as defaults. Make accepts `CAYRN_LFE_DIR` and
`XREPL_DIR` and sets those environment variables for its child test process.
The suite is Unix-only, matching the current host/socket profile.

## Preserved scenarios

| Behavior from the Python runner | Rust test(s) |
| --- | --- |
| Version, malformed/oversized frame and stale generation/authority rejection | `malformed_version_and_stale_native_frames_fail_closed` |
| Shared bindings, lost-reply dedup/conflict, Unicode, errors, unsupported input, last-client reconnect | `bindings_shared_clients_lost_reply_dedup_and_reconnect` |
| Busy evaluator leaves peer context and inspection responsive | `busy_context_does_not_block_peer_or_inspection` |
| Contiguous replay/follow and immutable late-output attribution | `late_output_and_follow_preserve_origin_and_contiguous_cursors` |
| Native interrupt confirmed by evaluator death; no delayed side effect; peer survives | `cancellation_confirms_death_blocks_side_effect_and_preserves_peer` |
| Busy close, capture barrier, repeated close, immutable archive and peer preservation | `busy_close_seals_capture_retains_archive_and_preserves_peer` |
| SQLite output abort never falsely ACKs captured output | `output_transaction_abort_never_false_acks_or_reexecutes` |
| SQLite dispatch-intent abort never executes | `dispatch_transaction_abort_never_executes_or_retries` |
| Unexpected evaluator death quarantines one context | `evaluator_death_quarantines_only_its_context` |
| SIGKILL host, surviving BEAM, committed history, unknown operation and no replayed effect | `host_crash_retains_history_and_unknown_work_without_reexecution` |
| Whole-node loss and retained sealed archive | `node_loss_marks_active_work_unknown_but_keeps_closed_archives` |
| Actual consumer, contiguous JSON events and EOF detach | `json_consumer_replays_contiguous_events_and_eof_only_detaches` |
| PTY draft survives peer output; quit preserves live state | `draft_survives_peer_output_and_quit_keeps_live_state` |
| Actual keyboard Ctrl-C through a controlling PTY; peer survives | `keyboard_ctrl_c_interrupts_evaluation_and_preserves_peer` |

Each case starts its own test-owned agent and host in a private temporary
directory. Child guards kill/reap owned processes on assertion failure; a failed
case prints the agent/host logs and, for PTY cases, captured terminal output.
Wire I/O, readiness, outcome polling and normal child exit have deadlines.
The PTY helper answers terminal cursor-position queries and sends the actual
Ctrl-C byte; it does not substitute a process signal for keyboard behavior.

These tests exercise transaction-abort injection, not physical disk-full, fsync
or power-loss durability. They do not establish independent acceptance, complete
T01–T13 conformance or production readiness. The runtime/ownership limits in
[the native-agent profile](native-agent.md) continue to apply.

## Verification

On 2026-10-18, `make check test-xrepl` passed: 16 ordinary Cayrn tests, all
14 Rust integration tests, 6 native EUnit tests, 7 xrepl tests and 1 xrepl doctest;
format, Clippy (including both optional features) and rustdoc gates passed.
Native-only Clippy also passed. The documented filtered
`make test-agent TEST_ARGS='host_crash --nocapture'` ran the selected test alone.
An intentionally missing `CAYRN_LFE_DIR` made an explicitly selected native test
fail with the expected missing-launcher diagnostic; it did not silently skip.
These results are author-attested local checks, not independent acceptance.
