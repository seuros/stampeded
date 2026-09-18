# Reference survey

27 repos cloned shallow to `~/Projects/ecosystem/reference/`, grouped by
substrate. Restore history per-repo with `git fetch --unshallow`.

(The directory already contained `biome`, `jiff`, `tidewave_rails` from earlier
work — unrelated to this survey.)

```
postgres/   oban river graphile-worker pg-boss procrastinate underway
            good_job solid_queue que db-scheduler neoq
polyglot/   faktory temporal nats-server
redis/      sidekiq asynq bullmq rq dramatiq resque
other/      celery hangfire jobrunr apalis machinery
stack/      rama datastar
```

## LICENCE WARNING — read before opening source

Stampeded is BSD-3 and clean-room. Reading strong-copyleft source and then
writing a competing implementation in the same problem space invites
contamination claims. This rule is load-bearing, not decorative.

### Restricted — documentation and protocol only, never source

| repo | licence | note |
|---|---|---|
| `polyglot/faktory` | **GPL-3.0** | the closest prior art, and the most dangerous to read. Use the protocol wiki. |
| `redis/dramatiq` | **GPL-3.0** | |
| `other/hangfire` | **GPL-3.0** | |
| `other/jobrunr` | **LGPL-3.0** | |
| `redis/sidekiq` | **LGPL-3.0** (OSS edition) | licence file did not match automated detection; treat as restricted |

### Safe to read

Apache / MIT / MPL / BSD:

`oban` (Apache), `river` (MPL), `temporal` (MIT), `graphile-worker` (MIT),
`pg-boss` (MIT), `procrastinate` (MIT), `underway` (Apache), `que` (MIT),
`solid_queue` (MIT), `db-scheduler` (Apache), `neoq` (MIT), `celery` (BSD),
`asynq` (MIT), `bullmq` (MIT), `apalis` (MIT), `machinery` (MPL),
`nats-server` (Apache), `rama` (Apache), `datastar` (MIT).

`good_job` licence file did not match detection — verify before reading.

Conveniently the permissive set covers the **entire Postgres cluster**, which is
where the design answers actually are.

## Reading priority

1. **`postgres/oban`** — best-in-class Postgres queue. Done: see `04-`.
2. **`postgres/river`** — independent take, same substrate. Done: see `04-`.
3. **`postgres/graphile-worker`** — third model (leases, not states). Done: see `04-`.
4. `postgres/underway` — the Rust one; closest to our stack.
5. `polyglot/temporal` — for how far the durable-execution model goes, and where
   to stop (we are explicitly not building this).
6. `postgres/procrastinate`, `postgres/pg-boss` — more Postgres variants.
7. `redis/asynq` — clean Go implementation, good for scheduler design.
8. `stack/rama`, `stack/datastar` — our own stack, read as usage docs.

## Findings so far

Captured in `04-job-lifecycle-and-claiming.md`. The highest-value single finding:
**Oban's CTE optimisation fence** (`basic.ex:119`) preventing the planner from
updating more rows than LIMIT — a silent correctness bug that would otherwise be
reinvented.

## Open question on rama

`rama` is a Tower-like modular service framework for proxies and network
services. A job processor's hard problems are storage, claiming, visibility and
retry semantics — almost none of which are network-middleware problems.

**Unresolved: what does `rama` buy over plain axum/tokio here?** It gives a nice
server surface for the part that is not hard. Worth answering explicitly before
committing, rather than adopting a framework whose strengths are orthogonal to
the bottleneck.
