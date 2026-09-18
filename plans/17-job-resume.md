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

| | what | survives a retry? |
|---|---|---|
| **BullMQ** | `updateProgress(json)` — `HSET` on the job hash (`job.ts:543`, `updateProgress-3.lua:27`) **[V]** | **yes**, nothing clears it — but it is documented as "Updates a job's progress" and emits a UI event. Accidentally a checkpoint |
| **JobRunr** | `JobDashboardProgressBar` **[V]** | dashboard only |
| **Hangfire** | `ContinuationsSupportAttribute` **[V]** | different feature — job *chaining* (run B after A), not resuming A |
| **Temporal** | event-sourced replay **[V]** | yes, and completely: the other end of the spectrum |
| everyone else in `08-` | nothing | — |

**Nobody in the Postgres cohort has it at all** — not Oban, River, good_job,
solid_queue, que, graphile, pg-boss, procrastinate, underway, neoq or
db-scheduler. BullMQ's is the closest prior art and it is a progress bar that
happens not to be cleared.

That is the gap. It is also a warning: the reason nobody ships this is that the
semantics below are genuinely awkward, not that nobody thought of it.

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

## 7. Still open

- **Nothing verifies the checkpoint is a cursor rather than a result set.** The
  64 KB cap is a blunt instrument that catches the pathological case and
  nothing else.
- **No help with idempotency.** Resuming from 50k assumes rows 1–50k were
  committed before the checkpoint was written. If the job checkpoints *before*
  committing its chunk, a crash between the two replays that chunk. The
  ordering is the job author's problem and cannot be fixed from here — but it
  should be stated loudly in the docs, because it is the one way to use this
  feature and get silent duplicates.
- **Interaction with `expire_seconds` is untested.** A job that checkpoints
  regularly can now never expire, since each checkpoint refreshes
  `heartbeat_at`. That is intended, but it means a job stuck in a checkpointing
  *loop* is invisible to every timeout layer.
- **Should the dashboard show it?** A cursor is often the single most useful
  number about a long job ("50,000 / 100,000"), but it is opaque JSON and we
  cannot know which field to render.
