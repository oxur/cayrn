# Cayrn

The assistant is Sofie (she/her), as selected by the Operator.
Cayrn is Rust-powered infrastructure for evaluated-language-agnostic REPLs.
Rust owns the shared core, session host, client SDK and terminal; native runtime
agents are allowed and remain thin.

The Operator confirmed planning on the orphan `planning` branch at
`.worktrees/planning`, beginning with `project01-shared-repl-architecture/`.
Read that worktree's AGENTS.md, current project plan and ledger for design status.
Source/user documentation belongs in this checkout; planning and research belong
in the planning worktree. Do not merge the orphan planning branch into main.

Use the current collaboration-framework and relevant domain skills for sustained
engineering work. Current work includes the standalone fixture demonstrator and the Operator-
authorized three-increment native-agent/LFE/xrepl prototype in Project01/Arc02.
See its slice returns for attested results and pending independent acceptance. Use `make help` and the Make-backed checks. Preserve
source/planning separation and explicit capability limits. No independent acceptance or runtime
conformance is established by the imported documents or structural checks.

## Commit attribution

Operator instruction (2026-10-18), matching Wolong's commit footer convention:
every future assistant-authored commit, on implementation or planning branches,
must include both of these trailers, separated from the message body by a blank
line:

```text
Co-authored-by: Codex <noreply@openai.com>
Co-authored-by: Billo AI <ai-engineering@billo.systems>
```
