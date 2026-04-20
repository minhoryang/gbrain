# MINIONS — Product Requirements Document

**Version:** 1.0  
**Status:** Shipped (v0.11.0+, refined through v0.14.0)  
**Audience:** Product, engineering, downstream integrators

---

## 1. Problem Statement

GBrain agents do meaningful work: syncing pages, extracting knowledge graph edges, enriching entities, bulk-importing files, running research loops. Before Minions, all of that happened inline inside a single LLM gateway session. That caused four compounding problems:

1. **No durability.** A gateway restart or network blip killed mid-flight work with no recovery path.
2. **No observability.** Users could not see what was running, how far along it was, or how many tokens it had consumed.
3. **No steerability.** Once a background task was running there was no way to redirect, pause, or cancel it without killing the entire agent session.
4. **Serial bottleneck.** Parallel research (5 companies at once) required the orchestrating agent to block on each one or spin up ad-hoc sessions with no coordination layer.

These are infrastructure problems, not capability problems. The solution is a durable job queue that the agent submits work into, not a smarter prompt.

---

## 2. Goals

| # | Goal |
|---|------|
| G1 | Jobs survive gateway restart. Anything submitted is reachable after reconnect. |
| G2 | Every job has structured, queryable progress and token accounting. |
| G3 | Running jobs can be redirected, paused, and cancelled at any time. |
| G4 | Parent-child DAGs let one orchestrator fan out N parallel agents and block until all finish. |
| G5 | Zero external dependencies. Runs on the same Postgres already used by the brain. |
| G6 | No footgun: agent callers (MCP) cannot submit privileged shell jobs without explicit operator opt-in. |
| G7 | Scheduler-grade polish: quiet-hours, deterministic stagger, configurable backoff. |

---

## 3. Non-Goals

- **Not a general-purpose workflow engine.** Minions does not implement sagas, compensating transactions, or event sourcing.
- **Not a distributed queue.** One Postgres instance, one or more workers on the same instance. No cross-region broker.
- **Not a real-time streaming system.** Progress polling is sufficient; WebSocket push is out of scope.
- **Not a sandbox.** The shell handler runs real child processes with real filesystem access. Security comes from operator configuration, not OS isolation.

---

## 4. User Personas

| Persona | How they use Minions |
|---|---|
| **Agent/LLM caller** | Submits jobs via `submit_job` MCP operation. Monitors via `get_job`, `list_jobs`. Steers via `send_job_message`. |
| **Cron operator** | Registers recurring shell jobs through `gbrain jobs submit shell`. Monitors the `gbrain jobs stats` dashboard. |
| **Brain maintainer (human)** | Runs `gbrain jobs work` as a daemon. Cancels stuck jobs. Prunes old completed jobs. |
| **Downstream integrator** | Embeds `MinionQueue` + `MinionWorker` in a custom TypeScript process. Registers domain-specific handlers. |

---

## 5. Core Concepts

### 5.1 Job

A row in `minion_jobs`. Has a `name` (handler type), `data` (JSONB payload), `queue`, `status`, and full retry/backoff metadata. Jobs are immutable once terminal (`completed`, `failed`, `dead`, `cancelled`).

### 5.2 Status Machine

```
waiting ──claim──► active ──complete──► completed
   ▲                  │
   │                  ├──fail (retries left)──► delayed ──promote──► waiting
   │                  │
   │                  └──fail (exhausted/unrecoverable)──► dead
   │
waiting-children ──all children done──► active (re-claimed by parent handler)
   ▲
   └──parent_job_id set on submit

paused ──resume──► waiting
active ──cancel──► cancelled
```

### 5.3 Worker

`MinionWorker` polls `minion_jobs` for claimable rows, runs handlers in an in-process Promise pool up to `concurrency`, renews lock tokens, and handles stall detection. Graceful shutdown: SIGTERM fires `shutdownAbort`, handlers with child processes (shell handler) run SIGTERM → 5s → SIGKILL cleanup.

### 5.4 Handler

A plain async function `(ctx: MinionJobContext) => Promise<unknown>`. Registered by name. The context gives the handler its payload, an `AbortSignal`, progress/token update methods, and a read-inbox channel for mid-flight steering.

### 5.5 Inbox

A `minion_job_inbox` table. Any caller with a job ID can post a message payload. The running handler polls `ctx.readInbox()` on each iteration and adjusts behavior. This is the steering primitive.

### 5.6 Parent-Child DAGs

When a job has a `parent_job_id`, the parent automatically enters `waiting-children` status and does not proceed until all children reach a terminal state. On child completion a `child_done` message is inserted into the parent's inbox. Child token counts roll up to the parent automatically.

---

## 6. Feature Requirements

### 6.1 Job Submission (P0)

- Submit a job by name and JSON data payload.
- Optional: `queue`, `priority`, `max_attempts`, `delay` (ms), `parent_job_id`, `on_child_fail`, `timeout_ms`, `idempotency_key`.
- Protected job names (`shell`) require explicit `allowProtectedSubmit` flag, never settable by MCP callers.
- Idempotency: same non-null `idempotency_key` returns the existing job, no second row.

### 6.2 Job Monitoring (P0)

- `getJob(id)` — full record with progress, result, token counts, error text, stacktrace log.
- `listJobs({ status, queue, limit })` — paginated job list.
- `getJobStats()` — per-queue, per-status counts plus dead job count.

### 6.3 Job Lifecycle (P0)

- `cancelJob(id)` — cascade-cancels all live descendants, inserts inbox notifications.
- `pauseJob(id)` — sets status=paused; job will not be claimed until resumed.
- `resumeJob(id)` — sets status=waiting.
- `retryJob(id)` — re-queues a dead/failed job with attempts reset.

### 6.4 Steer Running Job (P1)

- `sendJobMessage(id, sender, payload)` — inserts to inbox; validates sender.
- Handler calls `ctx.readInbox()` to receive messages; read receipts tracked.

### 6.5 Retry and Backoff (P0)

- Fixed or exponential backoff with configurable jitter.
- `UnrecoverableError` from handler skips all retries → dead immediately.
- Stall detection: lock token expiry → `handleStalled()` requeues or dead-letters.
- Per-job `timeout_ms` enforced cooperatively via `AbortSignal` + DB-side `handleTimeouts()`.

### 6.6 Parent-Child Fan-out (P1)

- Parent auto-enters `waiting-children` on first child submission.
- `on_child_fail` policy per child: `fail_parent`, `remove_dep`, `ignore`, `continue`.
- Max spawn depth configurable (default 5). Cap on live children per parent (`max_children`).

### 6.7 Token Accounting (P1)

- Each job tracks `tokens_input`, `tokens_output`, `tokens_cache_read`.
- Child token counts roll up to parent on `completeJob`.
- Handler calls `ctx.updateTokens({ input, output, cache_read })`.

### 6.8 Attachments (P2)

- Submit base64-encoded file bytes with a job.
- Validation: no path traversal, no null bytes, content_type well-formed, size capped (default 5 MiB), per-job filename uniqueness enforced at DB constraint.
- Retrieve via `getAttachment(jobId, filename)`.

### 6.9 Scheduler Polish (P2)

- **Quiet hours:** JSONB config on job specifying `{start, end, tz, policy}`. Evaluated at claim time. `defer` → release back to delayed queue with +15m offset. `skip` → cancel with `skipped_quiet_hours` reason.
- **Deterministic stagger:** `stagger_key` → FNV-1a hash → 0-59 minute offset. Jobs sharing a key (e.g., same cron fire) are decorrelated automatically. No thundering herd on minute boundaries.

### 6.10 Shell Handler (P2)

- Built-in `shell` handler: runs `/bin/sh -c cmd` or `argv[]` child process.
- Requires `GBRAIN_ALLOW_SHELL_JOBS=1` env on worker. Off by default.
- Env allowlist: `PATH HOME USER LANG TZ NODE_ENV` + caller-supplied `data.env` overrides. Prevents accidental API key leakage.
- Per-submission JSONL audit log at `~/.gbrain/audit/shell-jobs-YYYY-Www.jsonl`.
- Shutdown: SIGTERM → 5s grace → SIGKILL on child process.
- Stdout tail capped at 64 KB, stderr at 16 KB, UTF-8 safe.

### 6.11 Prune / Cleanup (P1)

- `prune({ olderThan })` — deletes completed/dead jobs older than the given date.
- `remove_on_complete` / `remove_on_fail` per-job flags for automatic cleanup after terminal transition.

---

## 7. Security Requirements

| Requirement | How satisfied |
|---|---|
| MCP callers cannot submit shell jobs | Protected name check in `MinionQueue.add()`. MCP sets no `allowProtectedSubmit` flag. |
| Whitespace bypass | Name normalized with `.trim()` before protected-name check. |
| Child env isolation | Shell handler uses an explicit allowlist; no `process.env` passthrough. |
| Audit trail | Shell job submissions logged to `~/.gbrain/audit/shell-jobs-YYYY-Www.jsonl`. Best-effort; documented as operational trace, not forensic proof. |
| Spawn-depth cap | Default max depth 5. Prevents agent recursion explosions. |
| Attachment traversal | Filename validation rejects `/`, `\`, `..`, null bytes. |
| Lock token fencing | All state transitions match `lock_token` to prevent split-brain double-execution. |

---

## 8. Operational Requirements

- Worker is a long-lived process (`gbrain jobs work`), poll-based, no LISTEN/NOTIFY dependency (PGLite compat).
- Stall + timeout detection runs on a configurable interval (default 30s).
- Graceful shutdown: 30s window for in-flight jobs to complete.
- `gbrain jobs smoke` verifies queue submission + worker round-trip.
- Statistics dashboard: `gbrain jobs stats`.

---

## 9. Success Metrics

| Metric | Target |
|---|---|
| Job durability on restart | 100% — no submitted job lost across worker restart |
| Stall recovery time | ≤ 2× `stalledInterval` (default ≤ 60s) |
| Steer latency (inbox → handler reads) | ≤ 1 handler iteration cycle |
| Token accounting accuracy | Exact; child rollup in same transaction as status flip |
| Shell job env leak incidents | 0 |

---

## 10. Out of Scope (Future Consideration)

- LISTEN/NOTIFY for sub-second job pickup (currently poll-based)
- Priority queues across multiple workers with a global scheduler
- Native cron expression support (currently driven by external cron + `delay` parameter)
- Job rate limiting per queue
- Cross-process worker mesh with shared state
