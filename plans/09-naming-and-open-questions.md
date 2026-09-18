# Naming, licensing, and open questions

## Naming status (verified 2026-09-16)

`stampede` is unavailable on both registries. The research below was done on
2026-09-16, when both looked like dormant squats; the crates.io name has since
been transferred to a new owner, which is what settled the question.

~~**DECIDED 2026-09-18: hold out for `stampede`.**~~ **SUPERSEDED the same
day — the project is `stampeded`.** The crates.io `stampede` name had just been
transferred to its current owner, so it is not a dormant squat any more and
there is nothing to adopt. Holding out for a name that has just changed hands
is waiting for something that is not going to happen.

**`stampeded` it is**, and it was the prepared fallback: free on both
registries as of 2026-09-18 (checked, not assumed), with the `d` suffix as the
daemon convention — `httpd`, `sshd`, `systemd` — so it reads as deliberate
rather than as a consolation prize. `stampede-rs` / `stampede-rb` and
`stampedeq` were also free, but two names for one project is worse than one
slightly odd one.

### The rename went further than this file recommended

This file argued for renaming the **crate, gem and binary only**, keeping
`stampede_jobs` / `stampede_claim`, the metric names and the notify channel,
on the grounds that a table prefix is not a package name and churning them
would invalidate every code sample.

**Overruled, deliberately, and the reasoning is worth keeping.** That argument
is strong for a *published* project, where renaming SQL identifiers is a
migration every user has to run. Nothing is published, so the cost it was
protecting against does not exist yet — and it never gets cheaper than now.
Against that, a package called `stampeded` whose tables are `stampede_jobs`
is a seam every reader has to be told about, in a project whose whole claim is
that the tables ARE the protocol. The identifiers people type are the API here;
having the API disagree with the package name would be the confusing kind of
consistency.

So: package, binary, Rust crate, Ruby module, SQL identifiers, notify channels
and `STAMPEDED_*` environment variables all moved together. 63 paths, ~110
files. The one thing deliberately NOT renamed is the English word: the README
still says the retry stampede "is what this is named for", which is the point
of the name.

### RubyGems — `stampede`

```
version   0.0.1
released  2013-03-07  (single version, never updated)
downloads 4,265
info      "Stampede makes dealing with application-related emails easier."
homepage  (none)
source    (none)
owner     joelvh
```

13 years dead, no repo, unrelated problem domain. Reclaimable via the
[RubyGems adoption process](https://guides.rubygems.org/rubygems-org-adoption/)
if the owner does not respond. Direct contact first — in progress.

### crates.io — `stampede`

```
version     0.0.0
description "placeholder"
downloads   1,417
repo        (none)
owner       gnufood  (Ethan Henderson)
```

A literal placeholder. crates.io policy explicitly covers names held by
placeholder crates with no published functionality, so this is a request to the
crates.io team rather than a negotiation.

### Also checked

| name | status |
|---|---|
| `walrus` (gem) | taken — 0.4, 2010, templating engine, owner `wincent`. 4 versions, real homepage — a stronger claim than stampede's. |
| `walrus-rb` | taken — 0.10.1.0, JS walrus.js port |
| `walrusdb` | **free** |
| `rama` (gem) | taken — 0.1.2, "Ruby project generator" (irrelevant, we want the crate) |

Renaming later is cheap — the constant and repo can change before 1.0. Not a
blocker.

## Licence

**BSD-3.** Clean-room. See `08-reference-survey.md` for which references may be
read as source and which are documentation-only.

## Open questions — the ones that actually change the design

### Architecture

- ~~What does `rama` buy over axum/tokio?~~ **DECIDED 2026-09-17: rama.**
  Owner's call; he is rama's #3 contributor. See `14-rust-stack.md`. The
  follow-on question that replaces it: **how does MCP work on rama**, given
  `rmcp` is hyperium-native and rama carries no `http` dependency.
- ~~Is the dashboard served by the daemon or a separate binary?~~ The daemon,
  behind `--http`.
- ~~Single daemon, or separate processes?~~ **One binary, roles behind flags.**
  And the split turned out to matter more than anticipated: `--no-worker` is
  exactly what lets a Rails app run the scheduler, reaper and pruner in Rust
  while its jobs stay in Ruby (`16-`).

### Schema and semantics

- ~~`jsonb` vs `bytea`~~ — jsonb. See `05-`.
- ~~Return values~~ — fire-and-forget, with the one bounded exception of step
  results (`17-`).
- ~~Multi-tenancy~~ — `tenant_id` column plus GCRA fairness.
- ~~Retention and partitioning~~ — resolved in `plans/README` §1: partition by
  QUEUE, not state, because uniqueness and state-partitioning are mutually
  exclusive. `pg_partman` not needed.
- ~~Unique jobs~~ — studied in `10-`; a generated `dedup_key` plus a partial
  unique index, with optional time bucketing.

### Delivery

- `LISTEN/NOTIFY` for wakeup vs polling interval vs both.
- Claim batch size; per-queue claiming vs global with fairness.
- Does `stampede_claim()` also register the heartbeat, or is that separate?

### Operational

- Connection pooling from day one (dev instance is stock: `max_connections=100`,
  `shared_buffers=128MB`).
- Dashboard auth **secure by default** — do not repeat the GoodJob-dashboard
  precedent from CommonShare, where auth was opt-in and staging/prod sat open.

## Decided

- PG18+ only, no compatibility shims
- Heartbeat-based orphan recovery, not time leases (`04-`)
- Postgres-as-protocol for enqueue; HTTP+SSE for clients outside the trust
  boundary (`05-`)
- Resilience comes from the four `*-machines` crates, not new code (`02-`)
- Lifecycle authoritative in Rust; triggers only for what the DB alone can do (`03-`)
