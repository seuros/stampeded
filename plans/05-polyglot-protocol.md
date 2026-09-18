# The polyglot boundary

The thing that makes Stampeded different. Also the thing most likely to sink it
if designed badly.

## The lesson from Faktory

Faktory's server is not its hard part. Its ongoing cost is ~10 client libraries,
mostly community-maintained, many stale. A custom TCP protocol means every
language needs someone to write and then keep maintaining a client. When that
stops happening, the polyglot promise quietly dies even though the server is
fine.

**So: the protocol choice dominates everything else in this project.**

(Faktory is GPL-3.0. Read its protocol wiki, never its source. See
`08-reference-survey.md`.)

## Proposal: Postgres is the protocol

Enqueue is an `INSERT`. That is the whole client library.

```sql
INSERT INTO stampeded_jobs (id, queue, task, args, scheduled_at)
VALUES (uuidv7(), 'default', 'send_email', '{"to":"x@y.z"}'::jsonb, now());
```

Consequences:

- **Every language already has a client**, because every language already has a
  Postgres driver. Zero client libraries to write or maintain. This is the
  entire Faktory tax, avoided.
- **Enqueue is transactional for free.** The job and the business data it refers
  to commit together or not at all. No lost jobs from a crash between COMMIT and
  enqueue — the failure mode that motivated the outbox work in CommonShare, and
  the one that had silently lost ~95k graph syncs there.

  **Caveat (verified on 18.6, see `10-unique-jobs.md` §6).** This holds cleanly
  at READ COMMITTED. At REPEATABLE READ or SERIALIZABLE, an `INSERT ... ON
  CONFLICT DO NOTHING` against the dedup index can still raise SQLSTATE **40001**
  — so a caller enqueueing a *deduplicated* job inside a stricter-isolation
  business transaction must be prepared to retry the whole transaction. Plain
  non-deduplicated inserts are unaffected. Do not oversell "just INSERT in your
  transaction" without this footnote; it is exactly the kind of detail that
  makes people distrust the rest of the pitch when they hit it in production.
- **No wire format to version, document or evangelise.**
- Inspection, admin, and migration are just SQL.

Thin idiomatic wrappers per language remain nice-to-have (`stampeded-rb`,
`stampeded-py`) but they are sugar over one INSERT, not protocol implementations.
If they rot, nothing breaks.

## Where raw SQL is not enough

Some clients cannot or should not hold a Postgres connection: browsers, edge
functions, third parties, anything across a trust boundary. For those, the Rust
daemon exposes **HTTP + SSE**:

- `POST /jobs` — enqueue
- `GET /jobs/:id` — status
- `GET /jobs/:id/events` — SSE stream of state transitions
- Auth at this layer; direct-SQL clients are inside the trust boundary already

HTTP is chosen over gRPC deliberately: every language speaks HTTP without a
codegen step, which is the same argument as choosing Postgres.

## The claim side is not symmetric

Enqueue is open to everyone. **Claiming is not.** Claiming requires the fence
logic, heartbeat registration, state-machine validation and retry accounting
from `04-job-lifecycle-and-claiming.md` — that is precisely where the subtle
correctness bugs live, and it must not be reimplemented per language.

Two options for polyglot *workers* (as opposed to polyglot enqueuers):

1. **`stampeded_claim()` SQL function.** One round trip, atomic, callable from
   any language. All the tricky logic stays in one place. A Python worker does
   `SELECT * FROM stampeded_claim('default', 5, 'worker-id')`.
2. **Daemon-mediated**: the Rust daemon claims and hands work out over HTTP/SSE.
   Workers never touch the claim logic at all.

Leaning toward **both** — (1) for trusted in-cluster workers that want no extra
hop, (2) for everything else. (1) is why the claim belongs in a SQL function
rather than in daemon-side Rust.

## Consequence for positioning

If this works, Stampeded's pitch is not "our protocol is better than Faktory's".
It is **"there is no protocol"** — you already know how to talk to it.

## Resolved by building

- ~~Job argument encoding.~~ **`jsonb`.** Inspectable in psql and searchable
  from `job_search`; `bytea` would have gutted the MCP surface.
- ~~Does `stampeded_claim()` register the heartbeat?~~ **No, separate.**
  Registration is an `INSERT` into `stampeded_workers` and the heartbeat is its
  own function, so a worker's liveness is independent of whether it currently
  holds a job — which is what lets the reaper distinguish "worker died" from
  "worker idle".
- ~~Return values.~~ **Fire-and-forget**, per `plans/README` §2. A job needing
  to report a result writes to its own table. Note the one exception that grew
  later: `steps` persist small step RESULTS, bounded at 8 KB, because a resumed
  job that cannot see what its completed steps produced is a footgun (`17-`).
- ~~Multi-tenancy.~~ **`tenant_id` column + GCRA fairness**, not
  schema-per-tenant. Composes with partitioning, and `tenant_id` can form part
  of `dedup_scope`.

## Still open

- **Daemon-mediated claiming** — option (2) above, handing work out over
  HTTP/SSE for workers across a trust boundary. **Scoped out, not pending.**
  The SQL-function path covers in-cluster workers, which is the common case,
  and building a second claim path would mean a second implementation of the
  semantics this file exists to keep singular.

  The limitation it leaves is real and should be said plainly rather than left
  implicit: **every worker needs database credentials.** A worker you do not
  trust with the jobs table cannot be a Stampeded worker. That rules out
  browser-side or customer-hosted workers, and it is a design consequence of
  "the tables ARE the protocol", not an oversight.

  One argument for it that this file did not have when it was written: the herd
  measurement in `12-wakeup-and-notify.md` prices broadcast wakeup at N-1 empty
  claims per job — 1.8 database cores at 50 workers and 1,000 jobs/s. A daemon
  handing work out knows which workers are idle, so it is also the answer to
  unicast-vs-broadcast, which makes it worth more than "for untrusted workers"
  suggests. Still not enough to justify a second claim path today, but the
  reason to revisit is now a number rather than a preference.
