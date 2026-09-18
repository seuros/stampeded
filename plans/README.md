# Stampede — plans

Design notes for a polyglot job processor: Rust core, Postgres 18+ substrate,
Datastar dashboard, MCP control plane. BSD-3, clean-room.

Written to avoid re-deriving the same analysis. Everything factual in here was
verified against a live system or a cloned source tree, not recalled — where a
claim came from executing something, the output is quoted.

## Index

| file | contents |
|---|---|
| `00-vision.md` | what it is, why, the three bets, non-goals |
| `02-crate-family.md` | the four `*-machines` crates and how each maps on |
| `03-postgres18.md` | PG18 features **verified on 18.6**, and what they buy |
| `04-job-lifecycle-and-claiming.md` | **the core decision.** Oban vs River vs Graphile, claiming and orphan recovery |
| `05-polyglot-protocol.md` | Postgres-as-protocol; why there is no client library |
| `06-dashboard-and-mcp.md` | Datastar surfaces, MCP tool design |
| `08-reference-survey.md` | 27 cloned repos, **licence warnings**, reading order |
| `09-naming-and-open-questions.md` | name status both registries, decided vs open |
| `10-unique-jobs.md` | uniqueness/dedup. **Uniqueness must be enforced by the index**, or the polyglot INSERT defeats it |
| `11-scheduling-and-cron.md` | delayed jobs, cron, leader election, DST, `pg_cron` verdict |
| `12-wakeup-and-notify.md` | polling vs LISTEN/NOTIFY; **trigger cost measured on 18.6** |
| `13-retention-and-bloat.md` | retention, autovacuum, index bloat, **partitioning decided** (LIST by state) |

(01, 07 and 10 intentionally unused — room to insert without renumbering.)

## Start here

If you read one file, read `04-job-lifecycle-and-claiming.md`. The claiming and
orphan-recovery decision determines the schema, and everything else follows from it.

## RESOLVED 2026-09-17 — the four decisions that unblock the schema

Owner's calls. All four verified against live 18.6 before being written down.

**1. Partition by queue, not by state.** `PARTITION BY LIST (queue)` with a
DEFAULT partition, and the dedup index led by `queue` — pg-boss's model, and why
it leads all six of its indexes with `name`. This keeps partitioning *and*
DB-enforced uniqueness, which the state-partitioning plan could not. Uniqueness
becomes **per-queue**, which is the semantic most callers want anyway.

Verified working:

```sql
CREATE TABLE stampede_jobs (
  id         uuid NOT NULL DEFAULT uuidv7(),
  queue      text NOT NULL,
  tenant_id  text,
  state      text NOT NULL DEFAULT 'available',
  dedup_scope text,
  dedup_key  bytea GENERATED ALWAYS AS (sha256(dedup_scope::bytea)) STORED
) PARTITION BY LIST (queue);

CREATE TABLE stampede_jobs_default PARTITION OF stampede_jobs DEFAULT;
CREATE UNIQUE INDEX ON stampede_jobs (queue, dedup_key) WHERE dedup_key IS NOT NULL;
```

Behaviour confirmed: duplicate `(queue, scope)` blocked; same scope on a
*different* queue allowed; `dedup_scope IS NULL` unconstrained, so non-unique
jobs are unaffected.

**Trap found while verifying:** `convert_to(text,'UTF8')` is **STABLE, not
IMMUTABLE**, so the `sha256(convert_to(...))` expression written in
`10-unique-jobs.md` **cannot be used in a generated column at all**. Use
`sha256(dedup_scope::bytea)` — the cast is immutable — or `decode(md5(...),'hex')`.
Both verified.

**Consequence to accept knowingly:** partitioning by queue means no state-based
pruning, so completed jobs live in the same partition as live ones. Bloat control
falls entirely to the pruner and autovacuum settings in `13-retention-and-bloat.md`,
which now matter more, not less. Queues are also created at runtime, so each new
queue needs `CREATE TABLE ... PARTITION OF` — pg-boss does exactly this
(`src/plans.ts:1439-1494`); the DEFAULT partition catches anything unprovisioned.

**2. No stored return values.** Fire-and-forget. A job needing to report a result
writes to its own table. Keeps rows small on the table that already has the
bloat problem. Reversible later; the reverse is not.

**3. Multi-tenancy: `tenant_id` column** + `throttle-machines` GCRA fairness, so
one noisy tenant cannot starve the rest. Composes with partitioning, and
`tenant_id` can form part of `dedup_scope`.

**4. Single daemon, roles configurable.** One binary running scheduler /
executor / API / dashboard behind flags; split later by running it with
different flags. Heed underway's trap (`14-rust-stack.md` §5): do **not** pin a
pooled connection per in-flight job.

## SUPERSEDED: partitioning and uniqueness are one decision, not two

`13-retention-and-bloat.md` recommends `PARTITION BY LIST (state)`.
`10-unique-jobs.md` recommends `UNIQUE (dedup_key) WHERE dedup_key IS NOT NULL`.

**These are mutually exclusive.** Verified on 18.6:

```
CREATE UNIQUE INDEX ON j (dedup_key) WHERE dedup_key IS NOT NULL;
ERROR:  unique constraint on partitioned table must include all partitioning columns
DETAIL:  ...lacks column "state" which is part of the partition key.
```

Adding `state` to the index is legal but changes the semantics to *per-state*
uniqueness, which is not uniqueness. A generated column also cannot itself be a
partition key. This is why pg-boss leads all six of its indexes with `name`.

Resolve this before writing the schema. Three ways out, none free: drop
partitioning, drop DB-enforced uniqueness, or partition on something the dedup
key can legally contain. Each sacrifices something a separate plan file argued
for, which is exactly why it must be decided once, deliberately, rather than
settled by whichever migration gets written first.

## The two things most likely to bite

1. **Licences.** Faktory (GPL-3.0) is the closest prior art and the most
   dangerous to read. Protocol docs only, never source. Full table in `08-`.
2. **Oban's CTE optimisation fence** (`basic.ex:119`). Without it the planner can
   update more rows than `LIMIT`, pushing `attempt` past `max_attempts`. We avoid
   the fence only because PG18's `RETURNING OLD./NEW.` removes the need for the
   subquery — that reasoning must live in the schema comments or someone will
   "simplify" it back into the bug.

## Status

**Built.** These notes are now design RATIONALE rather than a plan: every
recommendation in them is implemented, and where implementation contradicted a
note the note says so inline. The code is the source of truth; this directory
explains why it is shaped that way, which is the part that does not survive in
a diff.

Things that changed once they met a real database, all corrected in place:

- `13-retention-and-bloat.md` recommends `PARTITION BY LIST (state)`. It is
  partitioned by QUEUE — see §1 of the RESOLVED section above, which supersedes
  it. The consequence that file warns about (no state-based pruning, so the
  pruner and autovacuum carry the whole load) is real and is why the vacuum
  detector shipped.
- `11-scheduling-and-cron.md` said a spring-forward job should be skipped under
  `wall_clock`. It fires at the end of the gap instead — jiff's `Compatible`,
  and Vixie cron's behaviour. Skipping silently loses one run a year.
- The `sha256(convert_to(...))` expression in `10-unique-jobs.md` cannot be used
  at all (`convert_to` is STABLE). Noted there; the schema uses `::bytea`.
- The dedup time bucket needed three separate immutability facts that none of
  the notes had: two-argument `date_trunc` is STABLE, the three-argument form is
  IMMUTABLE, and `timestamptz::text` is STABLE again. See the comment on
  `dedup_key`.

Reference implementations: `~/Projects/ecosystem/reference/`

Our crates: `~/Projects/ecosystem/{state-machines-rs,chrono_machines,breaker_machines,throttle_machines}`
— all four are now used: `state-machines` for the lifecycle transition table,
`chrono-machines` for backoff, `breaker-machines` per destination, and
`throttle-machines` for both tenant fairness and wakeup pacing.
