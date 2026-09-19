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
**ANSWERED — in `14-rust-stack.md`, which exists because of this line.** That
file is the memo; this is the resolution, and the question's premise was false.

**It was never rama *or* axum. `mcp-host` is built on rama**, so rama is linked
either way. Verified:

```
$ cargo tree -i rama
rama v0.4.0
├── mcp-host v0.3.2
└── stampeded v0.1.0
```

`mcp-host`'s HTTP transport re-exports rama's own SSE type
(`transport/http.rs:63`, `pub use rama::http::sse::Event`). Choosing axum for
the dashboard would therefore have put **two HTTP stacks in one binary** — two
connection layers, two type systems, two CVE surfaces — to avoid the one the
control plane already links.

That also resolves the memo's §2.1, which made MCP the deciding row and then
concluded rama made MCP painful. It reached that by evaluating **`rmcp`**, the
hyperium-native official SDK — the wrong crate, as the memo's own header
records. With `mcp-host` the deciding row still decides, and decides the other
way.

**Do not lead with Datastar.** An earlier version of this answer did, and §1.5
dissolved that argument: rama has typed Datastar events and a 7,681-line
reference dashboard in-repo, but the official `datastar` SDK is axum-native, so
the row is a wash rather than a reason. Convenience once rama is a given, not
the justification.

**This section's own framing was the error.** "rama's strengths are orthogonal
to the bottleneck" is true — storage, claiming, visibility and retry semantics
are all in SQL — but it was used as an argument against adoption, when the
alternative was never *no framework*, only *a second one alongside it*.
