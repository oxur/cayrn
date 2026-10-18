# cayrn

[![][build-badge]][build]
[![][crate-badge]][crate]
[![][tag-badge]][tag]
[![][docs-badge]][docs]

[![][logo]][logo-large]

*Rust-powered infrastructure for language-agnostic REPLs, with persistent sessions,
shared terminal and editor access, and durable, replayable transcripts*

Cayrn is under development. It provides a local fixture session host and an
experimental [native-agent connector with real LFE evaluation](docs/native-agent.md).
Oxur, Lykn and Fennel integrations remain future work.

For a hands-on walkthrough of the real LFE integration, use the
[testing runbook](docs/testing-runbook.md), including checkout/branch locations,
two-terminal sessions, reconnect/replay, interruption and close.

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
./bin/cayrn serve-fixture \
  --state-dir /tmp/cayrn-demo \
  --worker ./bin/cayrn-fixture
```

In another terminal:

```sh
./bin/cayrn demo --state-dir /tmp/cayrn-demo
```

The demo opens two independent client connections, creates one session, adds 41
from the terminal client and 1 from the editor client, and checks both observe
the same transcript. The second result is 42. These are scripted clients;
Reedline and actual editor integrations come later. Exiting either client leaves
its session alive while the host/runtime remains healthy.

## Prototype requests

`request` takes one JSON operation and writes one JSON response. For example:

```sh
./bin/cayrn request --state-dir /tmp/cayrn-demo '{"op":"describe"}'
./bin/cayrn request --state-dir /tmp/cayrn-demo '{"op":"create"}'
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
of committed events. Cursor-based `follow` is also implemented; a separate push
subscription transport is not provided.
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
initial access boundary; there is no authenticated remote TCP service. Cursor
follow is implemented. Native agents negotiate interruption strength and managed
close-barrier support; the LFE profile supports session-destructive interruption
and close with retained read-only history. Fixture control returns unsupported.
Program input, archive deletion, other real language adapters, richer editor
services and an interactive Cayrn CLI remain future work. The xrepl Rust terminal
provides an experimental Reedline consumer of native sessions. No checkpoints,
automatic failover, production readiness or power-loss certification is claimed.
Records remain local and are not pruned automatically. See the native profile's
capture/ownership limits before using it with a shared BEAM application.

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

`make build` copies both `cayrn` and `cayrn-fixture` into `./bin` for easy access.
Use `make build-release` (or `make build MODE=release`) for optimized binaries.
`make format` and `make fmt` are aliases; `make lint` checks formatting and Clippy.
`make clean` removes the two copied binaries, while `make clean-all` also clears
Cargo build artifacts. Optional `make coverage` / `make coverage-html` require
`cargo-llvm-cov` and the toolchain's `llvm-tools-preview` component;
`make check-deps` requires `cargo-outdated`. `make deps` updates Cargo.lock within
the existing manifest constraints. Set `NO_COLOR=1` for plain Make output.
Help and build information show tracked edits as “uncommitted changes” and new
files as “untracked files”, combining both labels when needed.

Tests use actual Unix sockets, separate fixture processes and isolated temporary
state directories. Checks include format, workspace tests, Clippy with warnings
as errors and rustdoc. CI runs the same gates on Linux; local passing checks alone
do not establish a remote CI result. There is no coverage percentage claim.

Planning is maintained separately on the orphan `planning` branch/worktree.
The current project is `project01-shared-repl-architecture`; the first execution
slice is `arc01-session-demonstrator/slice01-durable-two-client-session`.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.

[//]: ---Named-Links---

[logo]: assets/images/v1.1-y250.png
[logo-large]: assets/images/v1.1-y1024.png
[build]: https://github.com/oxur/cayrn/actions/workflows/ci.yml
[build-badge]: https://github.com/oxur/cayrn/actions/workflows/ci.yml/badge.svg
[crate]: https://crates.io/crates/cayrn
[crate-badge]: https://img.shields.io/crates/v/cayrn.svg
[docs]: https://docs.rs/cayrn/
[docs-badge]: https://img.shields.io/badge/rust-documentation-blue.svg
[tag-badge]: https://img.shields.io/github/tag/oxur/cayrn.svg
[tag]: https://github.com/oxur/cayrn/tags
