# Cayrn

Rust-powered infrastructure for language-agnostic REPLs, with persistent sessions,
shared terminal and editor access, and durable, replayable transcripts.

Cayrn is under development. The first runnable increment is a **local fixture
session host**, not an evaluator for Oxur, LFE, Lykn or Fennel yet.

## Try the two-client demonstration

Requires a Unix host and Rust 1.95.0 (the initially tested toolchain), with a C
compiler for bundled SQLite. Toolchain components are declared in
`rust-toolchain.toml`.

```sh
make build
```

In one terminal, start the standalone host. Choose a new state directory, or an
existing directory with mode 0700; Cayrn will reject a permissive directory
rather than change its permissions.

```sh
target/debug/cayrn serve-fixture \
  --state-dir /tmp/cayrn-demo \
  --worker target/debug/cayrn-fixture
```

In another terminal:

```sh
target/debug/cayrn demo --state-dir /tmp/cayrn-demo
```

The demo opens two independent client connections, creates one session, adds 41
from the terminal client and 1 from the editor client, and checks both observe
the same transcript. The second result is 42. These are scripted clients;
Reedline and actual editor integrations come later. Exiting either client leaves
its session alive while the host/runtime remains healthy.

## Prototype requests

`request` takes one JSON operation and writes one JSON response. For example:

```sh
target/debug/cayrn request --state-dir /tmp/cayrn-demo '{"op":"describe"}'
target/debug/cayrn request --state-dir /tmp/cayrn-demo '{"op":"create"}'
```

Use the returned `id` and `generation` in later submissions:

```json
{"op":"submit","submission":{"session":"SESSION_ID","generation":"GENERATION_ID","operation":"my-first-op","client":"terminal","source":"add 7"}}
```

Inspect a submitted operation until it is no longer queued/dispatched:

```json
{"op":"inspect_operation","session":"SESSION_ID","operation":"my-first-op"}
```

Read events from a cursor; each client tracks its own contiguous processed prefix:

```json
{"op":"read","session":"SESSION_ID","after":"0","limit":32}
```

Repeat using `data.next` until it equals `data.head`. A cursor is a **decimal
string**, never a JSON floating-point number. Reads return a bounded ordered page
of committed events; live follow/push subscriptions are not implemented yet.
Submitting returns a durably queued or previously recorded operation, not a
promise that execution already finished. Admission can wait behind earlier work.

The fixture understands `get`, `add INTEGER`, `echo TEXT`, `sleep MILLISECONDS`
(0–1000), and `crash` (an intentional worker-failure test). Its counter is a checked
signed 64-bit integer; returned values are text. User-code errors do not imply
runtime loss. The worker protocol is internal fixture infrastructure.

## Current guarantees and limits

- One Rust host owns its private state directory through an OS file lock. Each
  session has a bounded submission queue, owning thread and dedicated process.
- Admission, the operation registry and accepted-event commit atomically.
  Dispatch intent commits before execution. Captured output/outcomes are journaled
  before observers can read them. SQLite uses WAL and `synchronous=FULL`.
- Retrying the same session/operation ID with the same full submission returns
  its existing record. Changed source, client or generation conflicts. Clients
  never automatically resubmit code after ambiguous network failure.
- Worker death records unknown active outcome and state loss. A host restart
  preserves committed records but marks its old sessions state-lost. It does
  **not** re-execute history or restore live heaps. Create a fresh session to
  execute new work; old records remain inspectable.
- Ctrl-C terminates the host and its owned fixture processes administratively.
  It is not the planned per-session close/archive operation. Ordinary client
  disconnect does not terminate a session.
- Initial limits: 8192 source/output/value bytes, 256 KiB encoded frames, 128 KiB
  event-page budget, 64 page rows, 64 connections, 64 live sessions and 128 pending
  admissions per session. A queue rejection is not durable acceptance.

This is an experimental Unix-only profile with a provisional JSON wire/schema.
Frame parsing rejects malformed/oversized payloads. The private directory is the
initial access boundary; there is no authenticated remote TCP service. Live
follow, cancellation, program input, session close/archive/delete, real language
adapters and Reedline are still future work. No checkpoints, automatic failover,
production readiness or power-loss certification is claimed. Records remain
local and are not pruned automatically; choose your state-directory location.

## Rust packages

| Package | Responsibility |
| --- | --- |
| `cayrn-protocol` | Semantic types, exact cursors and optional bounded wire framing |
| `cayrn-core` | Runtime-independent journal/execution policy and native runtime seam |
| `cayrn-client` | Correlated local RPC without automatic evaluation retries |
| `cayrn-host` | SQLite, process/session ownership and Unix socket service |
| `cayrn-cli` | Host entrypoint, request client and two-client demo |

The live runtime does not require `Send`: it is constructed and used on its
session's owning thread. Native runtime agents can later implement the thin
language-specific boundary while Rust retains common policy and durability.

## Development

```sh
make help
make check
```

Tests use actual Unix sockets, separate fixture processes and isolated temporary
state directories. Checks include format, workspace tests, Clippy with warnings
as errors and rustdoc. CI runs the same gates on Linux; local passing checks alone
do not establish a remote CI result. There is no coverage percentage claim.

Planning is maintained separately on the orphan `planning` branch/worktree.
The current project is `project01-shared-repl-architecture`; the first execution
slice is `arc01-session-demonstrator/slice01-durable-two-client-session`.
