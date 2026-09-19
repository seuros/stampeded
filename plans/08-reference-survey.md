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

~~**Unresolved: what does `rama` buy over plain axum/tokio here?**~~
**ANSWERED, and the framing above was wrong.**

The question assumed rama's strengths are network-middleware ones, which are
orthogonal to a job processor's bottleneck. That is true of most of rama and
irrelevant, because the one thing we use it for is not middleware:
**`rama::http::sse::datastar`** — first-class Datastar SSE, with
`PatchElements` and `ElementPatchMode` as types rather than strings we format
by hand.

The dashboard is Datastar (`06-`). axum has SSE but knows nothing about
Datastar, so on axum the patch protocol would be our own string formatting,
which is exactly the sort of thing that is fine until the protocol version
moves. That is not orthogonal to anything; it is the specific reason.

What we actually import, all of it:

```
rama::http::sse::datastar::{ElementPatchMode, PatchElements}
rama::http::sse::{Event, server::KeepAlive}
rama::http::service::web::{extract, response}
rama::http::header, rama::tcp::server, rama::layer, rama::rt
rama::futures::stream, rama::utils::str
```

A thin HTTP surface plus the Datastar types. The honest summary: rama is not
carrying the hard parts — storage, claiming, visibility and retry semantics are
all in SQL, exactly as this section says — and it was never going to. It is
carrying the dashboard's wire protocol, and it is the only Rust HTTP framework
that does.
