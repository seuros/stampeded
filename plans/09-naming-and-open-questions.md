# Naming, licensing, and open questions

## Naming status (verified 2026-09-16)

`stampede` is squatted on both registries. Neither is a live project.

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
- Is the dashboard served by the daemon or a separate binary?
- Single daemon, or separate scheduler / executor / api processes?

### Schema and semantics

- `jsonb` vs `bytea` for job args. Leaning **`jsonb`** — inspectable, queryable,
  and it is what makes the MCP `job_search` tool useful. `bytea` would gut the
  MCP surface.
- Does Stampede store job **return values**? Temporal does, Sidekiq does not.
  Changes retention and table size materially.
- Multi-tenancy: schema-per-tenant vs `tenant_id` column + `throttle-machines`
  fairness.
- Completed-job retention and partitioning — **blocked on `pg_partman` being
  absent from the current image** (see `03-postgres18.md`).
- Unique jobs: Oban's `unique` option vs River's `unique_key`. Not yet studied —
  next reading task.

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
