# Job resume

A job that dies at row 50,000 of 100,000 currently starts again at row 1. This
file works out what to do about that.

Everything marked VERIFIED was read out of a cloned implementation on
2026-09-18.

## 1. Two features share the name

They are not variations of one idea, and conflating them is how this gets
designed wrong.

**Checkpointing.** The job explicitly saves a cursor — "I have done 50,000" —
and on the next attempt reads it back and continues. The job author is
responsible for making the work resumable and idempotent. The framework stores
an opaque blob and hands it back.

**Durable execution.** The framework records the result of every step and, on
resume, *replays* the function, feeding recorded results back instead of
re-executing. The author writes ordinary-looking sequential code. Temporal,
DBOS and restate.dev work this way.

Durable execution is enormously more powerful and is **flatly incompatible with
`05-polyglot-protocol.md`**. To replay, the framework must intercept every side
effect, which means a deterministic execution model and a per-language SDK that
wraps every call the job makes. That is precisely the ~10-client-library tax
Faktory pays and that this project exists to avoid. A Ruby script doing
`INSERT INTO stampede_jobs` cannot participate in a replay engine, and never
will.

So: **checkpointing.** It is a column and a function, callable from any
language, and it composes with the polyglot claim instead of fighting it.

## 2. What the survey actually has

> **CORRECTION, and it matters.** The first draft of this file claimed nobody
> in the Postgres cohort had resumable jobs. **River has, and its design is
> better than the one below.** The error came from a search that silently
> matched nothing, followed by targeted greps for the features I already
> expected to find — confirmation bias with a broken tool. The corrected survey:

| | what | notes |
|---|---|---|
| **River** | `ResumableStep(ctx, name, fn)`, `ResumableStepCursor[T]`, `ResumableSetCursor[T]` (`resumable.go`) **[V]** | **the real prior art.** Named steps, fast-forwarded on resume; per-step typed cursors; state in the job's `metadata` jsonb |
| **Temporal** | event-sourced replay **[V]** | the other end of the spectrum |
| **BullMQ** | `updateProgress(json)` — `HSET` on the job hash (`job.ts:543`, `updateProgress-3.lua:27`) **[V]** | survives retry because nothing clears it, but documented as a UI progress bar |
| **JobRunr** | `JobDashboardProgressBar` **[V]** | dashboard only |
| **Hangfire** | `ContinuationsSupportAttribute` **[V]** | job *chaining* (run B after A), a different feature |
| Oban, good_job, solid_queue, que, graphile, pg-boss, procrastinate, underway, neoq, db-scheduler | nothing | — |

### What River does better

Three things, and one of them is a correctness property rather than ergonomics:

1. **Named steps.** `ResumableStep(ctx, "download", …)` then
   `ResumableStep(ctx, "transform", …)`. On resume, River fast-forwards through
   the steps already recorded as complete and re-enters the one that was
   running. A single opaque blob — what is implemented below — makes the job
   author reconstruct "where was I" by hand every time.
2. **Per-step typed cursors**, rather than one shared bag.
3. **`ResumableSetStepCursorTx(ctx, tx, job, cursor)` — the cursor is written
   INSIDE THE CALLER'S TRANSACTION.** This is the important one. It means the
   work and the record of the work commit together or not at all, which closes
   the hazard §7 of the first draft described as unenforceable.

(3) has been adopted: `checkpoint_on` in Rust, `checkpoint!(state, conn:)` in
Ruby, and under Rails it happens automatically because the checkpoint runs on
ActiveRecord's connection. (1) and (2) have not — see §8.

River also buffers `ResumableSetCursor` in memory and flushes with the job's
normal metadata write, so a tight loop costs no round trips. Ours writes
immediately, which is simpler and more durable but more expensive.

## 3. The shape

```sql
checkpoint      jsonb,          -- opaque to us; the job's own state
checkpoint_at   timestamptz,
checkpoint_seq  integer NOT NULL DEFAULT 0
```

and one function:

```sql
stampede_checkpoint(p_queue, p_id, p_worker_id, p_state jsonb) RETURNS integer
```

The checkpoint rides on the job row that `stampede_claim` already returns, so
resuming costs **no extra round trip** — the worker hands the handler whatever
state was last saved, or NULL on the first attempt.

### 3.1 A checkpoint is also a heartbeat

`stampede_checkpoint` sets `heartbeat_at` as well.

A job that just saved progress is demonstrably alive, so making the caller also
invoke `stampede_touch` would be asking them to say the same thing twice — and
the failure mode of forgetting is that a job which is plainly working gets
expired out from under itself by layer 2 of the timeout stack (`15-`). One
call, both meanings.

### 3.2 Ownership is checked, and that is not a formality

The `claimed_by = p_worker_id` predicate is the same one `complete` and `fail`
use, and it matters more here. The dangerous sequence:

1. worker A claims the job, checkpoints at 50k, then its host wedges
2. the reaper decides A is dead and returns the job to `retryable`
3. worker B claims it, resumes from 50k, checkpoints at 60k
4. **A wakes up** and tries to checkpoint at 51k

Without the predicate, A's stale write moves the cursor *backwards* and B's
next attempt redoes 9,000 units of work — or worse, if the job is not perfectly
idempotent, does them twice. With it, A's write matches no row and returns
NULL, which is how A finds out it no longer owns the job.

## 4. The awkward part: which budget does progress reset?

This is the question that makes the feature interesting, and there is no
answer that is right in every case.

A job processing 100k rows in chunks of 1k, on infrastructure that loses a node
every twenty minutes. Every attempt makes real progress. If `attempt` keeps
counting down, the job is discarded at `max_attempts` having completed 95% of
the work and having *never actually failed*. That is absurd. But if progress
refunds attempts unconditionally, a job that checkpoints once and then crashes
forever is immortal, and the poison-job brake that the whole schema is built
around (`04-`) stops braking.

**Decision: a checkpoint resets `interruptions`, and does not touch `attempt`.**

The two counters already mean different things (`15-`), and the distinction
answers this exactly:

- `interruptions` exists to stop a job that *kills workers* from walking the
  whole fleet, one process at a time. A job that checkpointed at 50k and then
  died is demonstrably not that job — it ran, it worked, and something else
  killed it. Resetting is correct, and it is what lets a long job survive a
  flaky afternoon.
- `attempt` bounds total work regardless. It still increments at claim, so a
  job that progresses one unit and then dies, forever, is still discarded at
  `max_attempts`. The brake still holds; it just no longer punishes success.

So progress buys tolerance of *infrastructure* failure, and buys nothing
against a job that is simply broken. Which is the distinction that matters.

## 5. Bounds, and where they live

**In SQL, not in the client.** que is the only surveyed implementation that
bounds error text database-side (`15-`), and it is the only bound that survives
a polyglot worker — a Python worker will not run our Ruby or Rust truncation.
Same argument, same place.

A checkpoint is the job's own state, so the temptation is to let it be
anything. It should not be: someone will store the 100,000 processed ids rather
than a cursor, and the row will be megabytes. **64 KB, CHECK-enforced**, with
an error that says to store a cursor rather than a set.

## 6. Lifecycle

- **Cleared on success**, like `dedup_scope`. A completed job's checkpoint is
  dead weight on the one table that already has the bloat problem (`13-`), and
  keeping it would contradict the fire-and-forget decision in `plans/README` §2.
- **Kept on failure**, obviously — it is the entire point.
- **NOT carried to the dead-letter queue.** A dead letter is a *new* job, and
  handing it a checkpoint would have it resume half-finished in a queue whose
  workers were never meant to run it. The provenance columns already record
  where it came from; someone re-driving can fetch the original if they want.

## 8. Still open

- **Named steps, per River.** The single opaque blob works, but it makes every
  job author write the same "where was I" reconstruction. River's step model is
  the better design and adopting it is a real API change, not a tweak: it needs
  a step registry, duplicate-name detection, and the fast-forward pass. Worth
  doing; not done.
- **In-memory cursor buffering, also per River.** Each `checkpoint` here is a
  round trip. A tight loop wants River's batching, at the cost of losing the
  last few updates on a hard kill.
- **Nothing verifies the checkpoint is a cursor rather than a result set.** The
  64 KB cap is a blunt instrument that catches the pathological case and
  nothing else.
- ~~**No help with idempotency.**~~ **SOLVED, by copying River.** The cursor
  can be written inside the caller's transaction, so the work and the record of
  it commit together. Under Rails this is automatic — the checkpoint runs on
  ActiveRecord's connection, so it is already inside whatever transaction the
  job opened. Elsewhere the connection is passed explicitly. The non-atomic
  form still exists for jobs that are idempotent anyway and would rather not
  hold a transaction open.
- **Interaction with `expire_seconds` is untested.** A job that checkpoints
  regularly can now never expire, since each checkpoint refreshes
  `heartbeat_at`. That is intended, but it means a job stuck in a checkpointing
  *loop* is invisible to every timeout layer.
- **Should the dashboard show it?** A cursor is often the single most useful
  number about a long job ("50,000 / 100,000"), but it is opaque JSON and we
  cannot know which field to render.
