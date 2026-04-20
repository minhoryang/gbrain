# MINIONS — Design Document

**Version:** 1.0  
**Status:** Shipped (v0.11.0+, refined through v0.14.0)  
**Audience:** Engineers implementing, maintaining, or porting Minions

---

## 1. Overview

Minions is a Postgres-native job queue and in-process worker for GBrain. It is inspired by BullMQ's API surface and Sidekiq's retry semantics but depends on nothing beyond the Postgres connection already used by the brain engine. It works with both embedded PGLite (WASM Postgres, no server) and standard Postgres+pgvector.

The three-layer structure:

```
┌─────────────────────────────────────────┐
│  MinionQueue   (SQL, no JS timers)      │
│  add / claim / complete / fail / prune  │
├─────────────────────────────────────────┤
│  MinionWorker  (in-process loop)        │
│  poll → claim → launch → complete/fail  │
├─────────────────────────────────────────┤
│  Handlers      (user-defined functions) │
│  (ctx: MinionJobContext) => Promise<T>  │
└─────────────────────────────────────────┘
```

`MinionQueue` is pure SQL operations. `MinionWorker` is the event loop. Handlers are user code. They are designed to be independently testable and independently replaceable.

---

## 2. Schema

### 2.1 `minion_jobs`

```sql
CREATE TABLE minion_jobs (
  id              BIGSERIAL PRIMARY KEY,
  name            TEXT NOT NULL,
  queue           TEXT NOT NULL DEFAULT 'default',
  status          TEXT NOT NULL DEFAULT 'waiting',   -- MinionJobStatus enum
  priority        INTEGER NOT NULL DEFAULT 0,
  data            JSONB NOT NULL DEFAULT '{}',

  -- Retry
  max_attempts    INTEGER NOT NULL DEFAULT 3,
  attempts_made   INTEGER NOT NULL DEFAULT 0,
  attempts_started INTEGER NOT NULL DEFAULT 0,
  backoff_type    TEXT NOT NULL DEFAULT 'exponential',
  backoff_delay   INTEGER NOT NULL DEFAULT 1000,     -- ms
  backoff_jitter  FLOAT NOT NULL DEFAULT 0.1,

  -- Stall detection / locking
  stalled_counter INTEGER NOT NULL DEFAULT 0,
  max_stalled     INTEGER NOT NULL DEFAULT 1,
  lock_token      TEXT,
  lock_until      TIMESTAMPTZ,

  -- Scheduling
  delay_until     TIMESTAMPTZ,

  -- Parent-child
  parent_job_id   BIGINT REFERENCES minion_jobs(id),
  on_child_fail   TEXT NOT NULL DEFAULT 'fail_parent',

  -- Token accounting
  tokens_input    BIGINT NOT NULL DEFAULT 0,
  tokens_output   BIGINT NOT NULL DEFAULT 0,
  tokens_cache_read BIGINT NOT NULL DEFAULT 0,

  -- v7 additions
  depth           INTEGER NOT NULL DEFAULT 0,
  max_children    INTEGER,
  timeout_ms      INTEGER,
  timeout_at      TIMESTAMPTZ,
  remove_on_complete BOOLEAN NOT NULL DEFAULT FALSE,
  remove_on_fail     BOOLEAN NOT NULL DEFAULT FALSE,
  idempotency_key TEXT,

  -- v12 additions (scheduler polish)
  quiet_hours     JSONB,
  stagger_key     TEXT,

  -- Results
  result          JSONB,
  progress        JSONB,
  error_text      TEXT,
  stacktrace      JSONB NOT NULL DEFAULT '[]',

  -- Timestamps
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at      TIMESTAMPTZ,
  finished_at     TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency dedup (partial: only non-null keys)
CREATE UNIQUE INDEX minion_jobs_idempotency_key_idx
  ON minion_jobs (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- Claim query covering index
CREATE INDEX minion_jobs_claim_idx
  ON minion_jobs (queue, status, priority, created_at)
  WHERE status IN ('waiting', 'delayed');
```

### 2.2 `minion_job_inbox`

```sql
CREATE TABLE minion_job_inbox (
  id         BIGSERIAL PRIMARY KEY,
  job_id     BIGINT NOT NULL REFERENCES minion_jobs(id) ON DELETE CASCADE,
  sender     TEXT NOT NULL,
  payload    JSONB NOT NULL,
  sent_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  read_at    TIMESTAMPTZ
);

CREATE INDEX minion_job_inbox_job_id_idx ON minion_job_inbox (job_id);
```

### 2.3 `minion_job_attachments`

```sql
CREATE TABLE minion_job_attachments (
  id           BIGSERIAL PRIMARY KEY,
  job_id       BIGINT NOT NULL REFERENCES minion_jobs(id) ON DELETE CASCADE,
  filename     TEXT NOT NULL,
  content_type TEXT NOT NULL,
  storage_uri  TEXT,            -- if stored to object storage
  content      BYTEA,           -- inline for small files
  size_bytes   INTEGER NOT NULL,
  sha256       TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (job_id, filename)     -- authoritative duplicate fence
);
```

---

## 3. Status Machine

```
                      ┌──────────────┐
           submit     │    waiting   │◄──── resume
           ───────►   └──────┬───────┘
                             │ claim
                             ▼
                      ┌──────────────┐
                      │    active    │
                      └──────┬───────┘
                    ┌────────┴──────────────┐
                    │                       │
           complete │               fail    │ abort signal
                    ▼                       ▼
             ┌──────────┐          ┌──────────────┐
             │completed │          │  delayed     │──promote──► waiting
             └──────────┘          │  (retry)     │
                                   └──────┬───────┘
                                          │ exhausted / UnrecoverableError
                                          ▼
                                   ┌──────────────┐
                                   │    dead      │──retry──► waiting
                                   └──────────────┘

  cancel: active/waiting/paused → cancelled (cascade to children)
  pause:  active/waiting → paused
  parent-fan-out: submit child → parent enters waiting-children → all children terminal → parent → waiting
```

### Terminal statuses

`completed`, `failed` (unused; maps to `dead` or `delayed`), `dead`, `cancelled`.

Only non-terminal jobs are claimable.

---

## 4. Core Algorithms

### 4.1 Claim (Pessimistic Locking)

```sql
UPDATE minion_jobs
SET status = 'active',
    lock_token = $lockToken,
    lock_until = now() + ($lockDuration || ' milliseconds')::interval,
    started_at = COALESCE(started_at, now()),
    attempts_made = attempts_made + 1,
    attempts_started = attempts_started + 1,
    updated_at = now()
WHERE id = (
  SELECT id FROM minion_jobs
  WHERE queue = $queue
    AND status = 'waiting'
    AND name = ANY($registeredNames)
    AND (delay_until IS NULL OR delay_until <= now())
  ORDER BY priority ASC, created_at ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

Key properties:
- `FOR UPDATE SKIP LOCKED` — concurrent workers cannot double-claim the same row.
- `attempts_made` incremented at claim time (not at fail time), so "how many times was this tried" is accurate even on stall.
- `lock_until` drives stall detection.

### 4.2 Complete (Token Rollup + Parent Resolution)

```sql
-- Single transaction:
-- 1. Flip job to completed, set result
UPDATE minion_jobs SET status='completed', result=$result, ... WHERE id=$id AND lock_token=$token;
-- 2. Roll up tokens to parent
UPDATE minion_jobs SET tokens_input = tokens_input + $childIn, ... WHERE id = $parentId;
-- 3. Insert child_done into parent inbox
INSERT INTO minion_job_inbox (job_id, sender, payload) VALUES ($parentId, 'system', $childDonePayload);
-- 4. Check if parent can be unblocked (no remaining non-terminal children)
UPDATE minion_jobs SET status='waiting' WHERE id=$parentId AND (no live children remain);
-- 5. remove_on_complete: DELETE job row if flag set
```

Everything in one transaction. A crash after step 3 but before step 4 would stall the parent indefinitely — this is the reason all five steps are inside a single `engine.transaction()`.

### 4.3 Fail + Parent Hook

```sql
-- Single transaction:
-- 1. Flip job to delayed/dead, set error_text
-- 2. Calculate backoff delay, set delay_until
-- 3. Apply on_child_fail policy:
--    fail_parent → flip parent to failed
--    remove_dep  → only remove child from dependency count
--    ignore/continue → noop
-- 4. remove_on_fail: DELETE row if flag set AND status is dead
```

### 4.4 Stall Detection

```sql
-- handleStalled():
-- Jobs stuck active past lock_until
UPDATE minion_jobs
SET stalled_counter = stalled_counter + 1,
    status = CASE WHEN stalled_counter + 1 > max_stalled THEN 'dead'
                  ELSE 'waiting' END,
    lock_token = NULL, lock_until = NULL, ...
WHERE status = 'active' AND lock_until < now();
```

Workers run this on `stalledInterval` (default 30s). Order of execution in the worker loop matters: stall detection before timeout handling, because `handleTimeouts` has a `lock_until > now()` guard that would skip already-expired locks.

### 4.5 Timeout Detection

```sql
-- handleTimeouts():
-- Jobs with an absolute timeout_at in the past
UPDATE minion_jobs
SET status = 'dead', error_text = 'timeout', ...
WHERE status = 'active'
  AND timeout_at IS NOT NULL
  AND timeout_at < now()
  AND lock_until > now();   -- still locked = still genuinely running
```

The JS-side `setTimeout` in the worker fires `abort.abort(new Error('timeout'))` cooperatively. The DB-side is the authoritative flip in case the JS side crashes.

### 4.6 Idempotency

```sql
-- On add(), if idempotency_key provided:
SELECT * FROM minion_jobs WHERE idempotency_key = $key;
-- If row found → return it, no insert
-- If not → proceed with insert
-- Unique partial index on (idempotency_key) WHERE NOT NULL enforces at DB level
```

### 4.7 Backoff Calculation

```typescript
// Fixed: delay = backoff_delay
// Exponential: delay = 2^(attempts_made - 1) * backoff_delay
// Jitter: delay += random * jitterRange * 2 - jitterRange  (±jitter%)
delay = Math.max(delay, 0);
```

Matches Sidekiq formula with BullMQ-style jitter parameter.

### 4.8 Quiet Hours (Claim-Time Gate)

Evaluated after claim, before handler launch. Runs `evaluateQuietHours(job.quiet_hours)`:

- Get local wall-clock hour via `Intl.DateTimeFormat` with job's configured `tz`.
- Check if hour falls in window (supports midnight wrap-around).
- `allow` → launch normally.
- `defer` → release lock, set `status='delayed'`, bump `delay_until = now() + 15m`.
- `skip` → route through `cancelJob()` (cascade-safe) rather than a bare UPDATE.

Why claim-time, not submit-time: a job submitted at 9am could become claimable at 11pm if there's backlog. Dispatch-time gating would incorrectly allow it.

### 4.9 Deterministic Stagger

FNV-1a hash of `stagger_key` mod 60 → minute offset in [0, 59]. Same key = same slot always. Used by the delayed-promotion path so N cron jobs scheduled for minute 0 get claim offsets of 0–59 minutes instead of all firing simultaneously.

```typescript
let h = FNV_OFFSET;  // 0x811c9dc5
for (let i = 0; i < key.length; i++) {
  h ^= key.charCodeAt(i);
  h = Math.imul(h, FNV_PRIME) >>> 0;  // 0x01000193
}
return h % 60;
```

---

## 5. Worker Loop

```
while (running):
  promoteDelayed()                // delayed jobs with delay_until <= now() → waiting
  
  if inFlight.size < concurrency:
    job = claim(lockToken, lockDuration, queue, registeredNames)
    
    if job:
      verdict = evaluateQuietHours(job.quiet_hours)
      if verdict != 'allow':
        handleQuietHoursDefer(job, lockToken, verdict)
      else:
        launchJob(job, lockToken)    // adds to inFlight Promise pool
    else if inFlight.size == 0:
      sleep(pollInterval)           // 5s default; PGLite doesn't have LISTEN
    else:
      sleep(100ms)                  // brief pause before re-checking free slots
  else:
    sleep(100ms)                    // at concurrency limit

  // Parallel: stalledInterval timer fires handleStalled() + handleTimeouts()
```

### Lock Renewal

Each in-flight job has a per-job `setInterval` at `lockDuration / 2`. On each tick:

```typescript
const renewed = await queue.renewLock(job.id, lockToken, lockDuration);
if (!renewed) abort.abort(new Error('lock-lost'));
```

If renewal fails (lock stolen by stall detection), the abort fires and the handler should wind down.

### Shutdown

```
SIGTERM → shutdownAbort.abort('shutdown')
         ↓
running = false  (exits poll loop after current iteration)
         ↓
await Promise.race([Promise.allSettled(inFlight), timeout(30s)])
         ↓
"Minion worker stopped."
```

Shell handler specifically listens to `ctx.shutdownSignal` to trigger its SIGTERM→SIGKILL kill sequence on child processes.

---

## 6. Handler Contract

```typescript
type MinionHandler = (ctx: MinionJobContext) => Promise<unknown>;

interface MinionJobContext {
  id: number;
  name: string;
  data: Record<string, unknown>;      // JSONB payload
  attempts_made: number;
  signal: AbortSignal;                // fires on timeout/cancel/lock-loss
  shutdownSignal: AbortSignal;        // fires on worker SIGTERM/SIGINT only
  updateProgress(progress: unknown): Promise<void>;
  updateTokens(tokens: TokenUpdate): Promise<void>;
  log(message: string | TranscriptEntry): Promise<void>;
  isActive(): Promise<boolean>;       // check if lock still held
  readInbox(): Promise<InboxMessage[]>;  // marks messages as read
}
```

**Return value:** Whatever the handler returns becomes `job.result` (serialized as JSONB). Return `undefined` for fire-and-forget.

**Error handling:**
- Throw `UnrecoverableError` → job goes directly to `dead`, no retry.
- Throw any other error → retry path (up to `max_attempts`).
- Check `ctx.signal.aborted` proactively in long loops; do not busy-wait.

---

## 7. Protected Job Names and Trust Boundary

`PROTECTED_JOB_NAMES` is a side-effect-free pure constant module (`Set<string>` = `{'shell'}`). It can be safely imported by the queue core without loading handler modules.

`MinionQueue.add()` calls `isProtectedJobName(name.trim())` before any DB work. If protected and `trusted?.allowProtectedSubmit !== true`, throws immediately.

The `trusted` parameter is a fourth argument to `add()`, not part of `opts`. This prevents user-supplied `{...opts}` spreads from accidentally carrying the flag.

Paths that set `allowProtectedSubmit: true`:
- `src/cli.ts` → all CLI invocations are local process → trusted.
- `src/core/operations.ts` `submit_job` operation when `ctx.remote === false`.

MCP callers (`src/mcp/server.ts`) set `ctx.remote = true`, never receive the flag.

---

## 8. Parent-Child DAG

```
submit(parent) → parent.status = 'waiting'
submit(child, parent_job_id=P) → 
  BEGIN:
    SELECT * FROM minion_jobs WHERE id=P FOR UPDATE  -- serialize on parent
    depth = parent.depth + 1
    if depth > maxSpawnDepth → throw
    count live children; if >= max_children → throw
    INSERT child with status='waiting' (or 'delayed')
    UPDATE parent SET status='waiting-children'
  COMMIT

child completes →
  BEGIN:
    UPDATE child status='completed'
    UPDATE parent tokens (rollup)
    INSERT child_done into parent inbox
    if no live children remain: UPDATE parent status='waiting'
  COMMIT

parent re-claims → runs parent handler → reads inbox → collects results
```

`on_child_fail` policies:
- `fail_parent` (default): flip parent to `failed` (propagates).
- `remove_dep`: remove child from dependency count; parent may still complete.
- `ignore`: child failure ignored; parent continues waiting for remaining siblings.
- `continue`: parent unblocked immediately, does not wait for this child.

---

## 9. Inbox / Steering

```
External caller:
  queue.sendMessage(jobId, sender, payload) →
  INSERT INTO minion_job_inbox (job_id, sender, payload)

Running handler:
  const msgs = await ctx.readInbox();
  →  SELECT * FROM minion_job_inbox WHERE job_id=$id AND read_at IS NULL FOR UPDATE;
     UPDATE minion_job_inbox SET read_at=now() WHERE id IN (...);
  handler injects messages as context for next iteration
```

The inbox doubles as the child-done notification channel. `ChildDoneMessage` has `type: 'child_done'`, `child_id`, `job_name`, and `result`.

---

## 10. Attachment Flow

```
queue.addAttachment(jobId, { filename, content_type, content_base64 }) →
  validateAttachment() → decode base64 → sha256 → size check → filename safety
  INSERT INTO minion_job_attachments

queue.getAttachment(jobId, filename) →
  SELECT content FROM minion_job_attachments WHERE job_id=$1 AND filename=$2
  → returns Buffer
```

Validation (pure function, `validateAttachment.ts`):
1. Non-empty filename.
2. No `/`, `\`, `..`, null bytes in filename.
3. Content-type matches RFC grammar regex.
4. Non-empty base64 string, strict charset (no whitespace).
5. Decoded size ≤ `maxAttachmentBytes` (default 5 MiB).
6. Not a duplicate filename for this job (fast pre-check; DB UNIQUE is authoritative).

---

## 11. Shell Handler Security Model

Two independent gates:

**Gate 1 (submission):** `PROTECTED_JOB_NAMES` check in `MinionQueue.add()`. MCP callers cannot pass `allowProtectedSubmit`. CLI callers can.

**Gate 2 (execution):** `GBRAIN_ALLOW_SHELL_JOBS=1` env var on the worker process. Without this, `registerBuiltinHandlers()` does not register the shell handler. Queued shell jobs stay in `waiting` forever (no handler to claim them).

**Env isolation:** `buildChildEnv()` picks only `['PATH', 'HOME', 'USER', 'LANG', 'TZ', 'NODE_ENV']` from `process.env`, plus explicit caller `data.env` overrides. No `...process.env` passthrough.

**Audit log:** Per-submission JSONL at `~/.gbrain/audit/shell-jobs-YYYY-Www.jsonl`. Never logs `env` values (may contain secrets). Logs `cmd` truncated to 80 chars or `argv` as JSON array. Best-effort: `appendFileSync` failure goes to stderr, submission continues.

**Process supervision:** `spawn('/bin/sh', ['-c', cmd])` uses absolute `/bin/sh` path so a poisoned `PATH` in `data.env` cannot redirect to a different shell binary. Abort (either `ctx.signal` or `ctx.shutdownSignal`) fires SIGTERM → 5s (`KILL_GRACE_MS`) → SIGKILL.

---

## 12. Token Accounting

All token accumulation is additive:

```sql
UPDATE minion_jobs
SET tokens_input = tokens_input + $input,
    tokens_output = tokens_output + $output,
    tokens_cache_read = tokens_cache_read + $cache_read,
    updated_at = now()
WHERE id = $id AND status = 'active' AND lock_token = $token;
```

On `completeJob()`, child totals roll up to parent in the same transaction:

```sql
UPDATE minion_jobs
SET tokens_input = tokens_input + $childIn,
    tokens_output = tokens_output + $childOut,
    tokens_cache_read = tokens_cache_read + $childCache
WHERE id = $parentId;
```

The lock-token fence prevents a double-update if the lock was lost and reclaimed.

---

## 13. PGLite Compatibility

Minions uses only standard SQL features. No LISTEN/NOTIFY (not in PGLite). Poll-based worker loop with configurable `pollInterval` (default 5s for PGLite, can be reduced for Postgres). `FOR UPDATE SKIP LOCKED` is supported by PGLite's embedded Postgres 17.5.

`engine.transaction()` is the abstraction layer. Both PGLite and Postgres engines implement it. `executeRaw<T>()` for arbitrary parameterized SQL.

---

## 14. Built-in Handlers

Registered by `registerBuiltinHandlers(worker, engine)` in `src/commands/jobs.ts`:

| Handler name | What it does |
|---|---|
| `sync` | Pull + embed new pages from the repo |
| `embed` | Re-embed pages (by slug or `all: true`) |
| `lint` | Run page linter with optional `--fix` |
| `import` | Bulk import markdown from a directory |
| `extract` | Extract links + timeline entries (`mode: "all"`) |
| `backlinks` | Check or fix back-links (`action: "fix"`) |
| `autopilot-cycle` | One full autopilot pass (sync+extract+embed+backlinks) |
| `shell` | Run an arbitrary command (requires `GBRAIN_ALLOW_SHELL_JOBS=1`) |

---

## 15. API Surface (TypeScript)

```typescript
// Queue
const queue = new MinionQueue(engine, { maxSpawnDepth?: number, maxAttachmentBytes?: number });
queue.add(name, data?, opts?, trusted?) → Promise<MinionJob>
queue.getJob(id) → Promise<MinionJob | null>
queue.listJobs(opts) → Promise<MinionJob[]>
queue.cancelJob(id) → Promise<boolean>
queue.pauseJob(id) → Promise<boolean>
queue.resumeJob(id) → Promise<boolean>
queue.retryJob(id) → Promise<MinionJob | null>
queue.sendMessage(id, sender, payload) → Promise<InboxMessage>
queue.getStats() → Promise<QueueStats>
queue.prune(opts) → Promise<number>
queue.addAttachment(jobId, input) → Promise<Attachment>
queue.getAttachment(jobId, filename) → Promise<Buffer | null>

// Worker
const worker = new MinionWorker(engine, opts?);
worker.register(name, handler) → void
worker.start() → Promise<void>   // blocks until SIGTERM
worker.stop() → void

// Errors
throw new UnrecoverableError('message')  // → dead immediately, no retry
```

---

## 16. Key Design Decisions

| Decision | Rationale |
|---|---|
| Postgres-native, no external broker | Brain already uses Postgres. Zero new infrastructure for the operator. Works with PGLite (embedded, zero-config). |
| `FOR UPDATE SKIP LOCKED` | Standard SKIP LOCKED pattern for horizontal worker scaling. Multiple workers on the same queue, no double-claim. |
| Lock token fencing on all state transitions | Prevents split-brain: stall detection and the worker can both race to flip a job's status; the lock token ensures exactly one wins. |
| `trusted` as 4th arg, not inside `opts` | Prevents `{ ...opts, allowProtectedSubmit: true }` spread attacks from user-controlled payloads carrying elevated trust. |
| `shutdownSignal` separate from `signal` | Non-shell handlers must not be cancelled on deploy restart (they get 30s to finish naturally). Shell handler needs to clean up its child process. Two signals, two policies. |
| Claim-time quiet hours, not submit-time | A job submitted at 9am may sit in the queue until midnight. Submit-time would permit it; claim-time enforces correctly. |
| All-in-one transactions (complete/fail/resolveParent) | Eliminates the crash window between child status flip and parent unblock. No stale `waiting-children` parents from a crashed worker. |
| FNV-1a for stagger | Tiny (15 lines), deterministic across runtimes, no dependency. 1/60 collision rate is acceptable for the 60-bucket problem. |
| `UnrecoverableError` convention | Handler signals "this will never succeed" (bad input, missing config). Skip all retries. Avoids burning `max_attempts` on jobs that will always fail. |

---

## 17. Testing Strategy

Unit tests (`test/minions.test.ts`):
- Full state machine coverage (all status transitions)
- Backoff calculation (fixed, exponential, jitter)
- Stall detection and stalled_counter rollover
- Dependency resolution (parent/child, all four `on_child_fail` policies)
- Lock mechanics (claim, renew, token mismatch)
- Depth and child-cap enforcement
- Timeout firing (JS-side abort)
- Idempotency key dedup
- `child_done` inbox insertion
- `removeOnComplete` / `removeOnFail` row deletion
- Attachment validation (path traversal, null byte, oversize, duplicate)
- Quiet-hours evaluation (in-window, out-window, midnight wrap, unknown tz)
- Protected-name check (whitespace bypass)

E2E tests (Postgres): 9 dedicated cases in `test/e2e/` covering the postgres-engine unnest binding path for batch operations.

Smoke test: `gbrain jobs smoke` — submits a job, waits for completion, verifies result round-trip.

---

## 18. Porting Guide (for relayd or other consumers)

To reuse Minions outside GBrain, the minimum surface needed is:

1. **Schema:** Apply `minion_jobs`, `minion_job_inbox`, `minion_job_attachments` DDL (see §2).
2. **Engine adapter:** Implement two methods:
   - `executeRaw<T>(sql: string, params: unknown[]): Promise<T[]>`
   - `transaction<T>(fn: (tx: Engine) => Promise<T>): Promise<T>`
3. **Queue:** `src/core/minions/queue.ts` — pure SQL, no GBrain-specific imports except the engine interface.
4. **Worker:** `src/core/minions/worker.ts` — imports queue + types only.
5. **Types:** `src/core/minions/types.ts` — no external deps.
6. **Backoff:** `src/core/minions/backoff.ts` — pure function.
7. **Quiet hours:** `src/core/minions/quiet-hours.ts` — pure function, `Intl.DateTimeFormat` only.
8. **Stagger:** `src/core/minions/stagger.ts` — pure function, FNV-1a.
9. **Attachments:** `src/core/minions/attachments.ts` — `node:crypto` only.
10. **Protected names:** `src/core/minions/protected-names.ts` — pure constant.

The shell handler (`handlers/shell.ts`, `handlers/shell-audit.ts`) is optional. It can be omitted entirely if shell execution is not needed. If included, the two-gate model (protected name + env flag) must be preserved.

The GBrain-specific concept in `queue.ts` is the `ensureSchema()` check against `engine.getConfig('version')`. Replace with your own migration guard or remove entirely if you manage schema separately.

Total portable core: ~1,200 lines of TypeScript, zero npm dependencies beyond Node built-ins and Postgres driver.
