# The polyglot boundary

The thing that makes Stampede different. Also the thing most likely to sink it
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
INSERT INTO stampede_jobs (id, queue, task, args, scheduled_at)
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

Thin idiomatic wrappers per language remain nice-to-have (`stampede-rb`,
`stampede-py`) but they are sugar over one INSERT, not protocol implementations.
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

1. **`stampede_claim()` SQL function.** One round trip, atomic, callable from
   any language. All the tricky logic stays in one place. A Python worker does
   `SELECT * FROM stampede_claim('default', 5, 'worker-id')`.
2. **Daemon-mediated**: the Rust daemon claims and hands work out over HTTP/SSE.
   Workers never touch the claim logic at all.

Leaning toward **both** — (1) for trusted in-cluster workers that want no extra
hop, (2) for everything else. (1) is why the claim belongs in a SQL function
rather than in daemon-side Rust.

## Consequence for positioning

If this works, Stampede's pitch is not "our protocol is better than Faktory's".
It is **"there is no protocol"** — you already know how to talk to it.

## Open questions

- Job argument encoding: `jsonb` (queryable, indexable, inspectable in psql) vs
  `bytea` (opaque, fast, language-native serialisation). Leaning `jsonb` —
  inspectability is a feature, and it makes the MCP control plane far more useful.
- Does `stampede_claim()` handle heartbeat registration, or is that a separate
  call?
- Result/return values: does Stampede store job return values at all, or is a
  job fire-and-forget? (Temporal does; Sidekiq does not. Storing results changes
  retention and table size significantly.)
- Multi-tenancy: schema-per-tenant, or a `tenant_id` column with
  `throttle-machines` enforcing fairness?
