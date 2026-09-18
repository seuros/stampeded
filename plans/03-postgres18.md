# Postgres 18+ — what it buys, verified

PG18+ only, deliberately. No compatibility shims for older versions. Every
reference implementation supports PG12-ish and therefore cannot use any of this.

All features below were **executed against a live 18.6 instance**
(`commonsharecore-db-1`, Debian 18.6-1.pgdg12+2), not read from release notes.

## Verified available

### `uuidv7()` — native, no extension

```
SELECT uuidv7();  -->  01a0ab66-12e0-7036-8f00-f1ee165a298d
```

Time-ordered primary keys for free. Gives index locality on insert *and* a
natural drain cursor, which is what a `bigserial` is normally carried for. No
sequence contention, no client-side generation, no `pgcrypto`.

### `RETURNING OLD.` / `NEW.` — the sleeper feature

```sql
UPDATE t SET state='claimed' WHERE id=1
RETURNING OLD.state AS was, NEW.state AS now;   -->  available | claimed
```

Claiming a job means "update it to claimed, but tell me what it was, so I can
detect that someone else got there first". Every reference does this with a CTE
round-trip or an optimistic version column. On 18 it is one statement.

It also composes with `state-machines`: the transition guard receives the before
and after state atomically, from the database, in the same statement that
performed the write.

**Oban, River and Graphile Worker cannot use this** because they support older
Postgres. This is the clearest single place where PG18-only pays.

### `GENERATED ... VIRTUAL`

```sql
CREATE TABLE g(a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL);  -->  21 | 42
```

Graphile Worker uses a **stored** generated column for `is_available`, costing
disk on every row. Virtual gives the same expressiveness at zero storage.

**CORRECTION (verified 18.6).** An earlier draft of this file claimed virtual
generated columns could express `is_ready` / `is_stuck` / `is_retryable`. **They
cannot.** Generation expressions must be IMMUTABLE, and every one of those
predicates needs `now()`:

```sql
CREATE TABLE t(scheduled_at timestamptz,
               is_ready boolean GENERATED ALWAYS AS (scheduled_at <= now()) VIRTUAL);
-- ERROR:  generation expression is not immutable
```

What DOES work is the time-independent shape — which is exactly why Graphile's
`is_available` is legal:

```sql
is_available boolean GENERATED ALWAYS AS
  (locked_at IS NULL AND attempts < max_attempts) VIRTUAL   -- OK
```

**SECOND CORRECTION (verified 18.6).** Even the legal shape above cannot be
used the way Graphile uses theirs, because **virtual generated columns cannot
be indexed**:

```sql
CREATE TABLE g(a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL);
CREATE INDEX ON g(b);
-- ERROR:  indexes on virtual generated columns are not supported
```

Graphile's `is_available` is **STORED** precisely because it is the predicate of
both fetch indexes (`graphile-worker/sql/000011.sql:67,71-81`). Virtual buys
nothing on the claim path: it must be STORED (paying the disk and the
non-HOT update) or the predicate must be written inline in the partial index.
See `13-retention-and-bloat.md` §7.

So the rule is: **generated columns for predicates over column values only;
anything involving the clock stays a query predicate or a partial index; and
anything that needs an index must be STORED, not VIRTUAL.**
Time-dependent readiness must be expressed as `WHERE scheduled_at <= now()` in
the claim, with a partial index supporting it — see `11-scheduling-and-cron.md`,
which measured Oban's missing partial index at 18,430 buffers / 54 ms versus
11 buffers / 0.165 ms.

### `pg_cron` 1.6 — available (not yet installed)

Recurring jobs scheduled inside the database. The schedule then survives the
workers being down entirely, which is a real reliability property, not a
convenience.

> **CORRECTED, per `13-retention-and-bloat.md` and confirmed twice while
> building.** `GENERATED … VIRTUAL` is NOT a free replacement for a stored
> column on the claim path, for two independent reasons: a virtual column
> **cannot be indexed** (`ERROR: indexes on virtual generated columns are not
> supported`), and a generation expression must be IMMUTABLE, so anything
> involving `now()` is rejected outright. Both were hit in practice. Virtual
> is fine for dashboard-facing derived fields and useless for the thing
> Graphile uses a stored column for — being the index predicate.

## Verified NOT available

- **`pg_partman` is absent from this image.** So are **`pg_repack`** and
  **`pg_squeeze`** (re-verified 18.6). **Resolved** in
  `13-retention-and-bloat.md`: we do not add the extension and we do not
  time-partition. Two LIST partitions by state, created once at install, no
  runtime partition DDL at all.
- **Indexes on virtual generated columns** — see the correction above.

Also present: `pg_stat_statements` 1.12, `pg_trgm` 1.6, `pgcrypto` 1.4,
`unaccent` 1.1, and **`pgstattuple` 1.5** (available, not installed — exact
index-bloat measurement, which pg-boss had to approximate because it could not
assume the extension).

## What PG18 does NOT solve

**Orphan recovery.** A worker dying mid-job is still a problem the application
must solve with a heartbeat or a lease. There is no database feature that
notices a process died. See `04-job-lifecycle-and-claiming.md`.

## Design stance on "use every PG feature"

Leverage, yes. Unlimited, no.

**Functions: yes, for operations.** The claim operation as a function is one
round trip, atomic, and callable from *any language* — which is the polyglot
story. Good.

**Triggers: narrowly.** Putting lifecycle enforcement in triggers actively
fights `state-machines` — the transition rules end up in two places, in two
languages, one type-checked and one not. The first divergence between them is a
bad afternoon. Use triggers only for what the database alone can do atomically
(counter maintenance, invariants the daemon cannot observe). Lifecycle stays
authoritative in Rust.

**Business logic in PL/pgSQL: no.** Functions are an API surface, not an
application tier.

## Operational note

The dev instance runs stock Docker defaults: `max_connections=100`,
`shared_buffers=128MB`. A Postgres-substrate queue lives or dies on connection
handling. Pooling (pgbouncer, or a pooled dispatcher in the daemon) belongs in
the design from day one, not bolted on after the first load test.
