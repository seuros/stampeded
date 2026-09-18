# Dashboard (Datastar) and MCP control plane

## Dashboard: Datastar

`starfederation/datastar` — hypermedia framework, ~5k stars, TypeScript, SSE-driven.
Cloned at `~/Projects/ecosystem/reference/stack/datastar`.

### Why it fits

A job dashboard is ~90% "stream state changes into a table". That is exactly
SSE's shape. The alternatives in this space:

- Sidekiq Web — ERB plus polling
- GoodJob — Rails engine plus Turbo
- River UI — a React SPA with its own build pipeline

Datastar gives live updates with no SPA build step and no polling. For a Rust
daemon that already has to expose HTTP, adding SSE is nearly free, and there is
no Node toolchain in the deployment.

### Surfaces

> **BUILT**, except where noted. One caveat that only became visible on
> implementing it: **breaker state cannot be read from the dashboard.**
> `breaker-machines` state lives in the worker process's memory, and the
> dashboard is normally served by a `--no-worker` daemon — a different process,
> usually a different machine. Nothing can see another process's breaker.
>
> So the destinations panel shows the EVIDENCE a breaker reacts to, derived from
> the job rows: how many jobs for each destination a worker refused
> (`breaker_open`, which is recorded when a circuit IS open), how many were
> throttled, how many genuinely failed, how many are waiting. That answers the
> operator's actual question — "is this destination sick?" — without pretending
> to a liveness it does not have.
>
> Making it a true live read would mean workers publishing breaker state to a
> table on each heartbeat. Worth considering; not done, and not obviously worth
> the write.


- ~~queue depth over time, per queue~~ — depth and oldest-waiting age, live.
  Not *over time*: a history needs a time series and somewhere to keep it.
- ~~jobs by state, live~~ — plus a filterable job list, and failure clusters by
  fingerprint, which is the panel that turns "47 jobs failed" into one row.
- ~~a job's transition history, streamed~~ — click any job id. Shows the
  structured `errors` ring buffer with exit statuses annotated (137 = OOM),
  plus the interruption and deferral budgets.
- ~~worker heartbeats~~ — with the reaper's `dead_after` flagged before it fires.
- ~~retry/backoff visualisation~~ — the next six attempts, read from the policy
  ON THE ROW rather than re-deriving the formula, which is why underway persists
  it. Labelled as an UPPER BOUND: the worker uses full jitter, so each retry
  lands somewhere below the figure shown. Saying "in 4m" when the truth is "in
  up to 4m" is a lie an operator would plan around.
- ~~circuit breaker and throttle state~~ — as *observed*; see the caveat above.

Also built, and not anticipated here because the features did not exist yet:
schedules with their cron rule and DST policy, resume progress (step names and
cursors) per job, and autovacuum-behind warnings.

That last one is worth emphasising: because resilience lives in our own crates,
the dashboard can show breaker state and rate-limit headroom, which Sidekiq and
Oban cannot show without their paid tiers.

### Open

- ~~Does the daemon serve the dashboard directly?~~ It does, behind `--http`.
- ~~Auth model.~~ **Opt-out, not opt-in**: no `--dashboard-token`, no dashboard,
  and the daemon says so at startup. Unauthenticated requests get 404 rather
  than 401 — an anonymous caller has no business learning a dashboard is here.
  The token rides on the query string because `EventSource` cannot set headers,
  which puts it in server logs and browser history; that cost is written into
  the module docs rather than glossed.
- **Panels are shared between clients, except the ones carrying a token.**
  Each connected client was re-running every query every second for
  byte-identical output, so ten operators meant ten times the load — and a
  dashboard gets opened by several people at once precisely during an incident.
  The four expensive panels (destination health, failure clustering, schedules,
  vacuum stats) are now rendered once per second and shared.
  
  `queues`, `jobs` and `job` are deliberately excluded: they embed filter links
  carrying the requesting client's TOKEN, so sharing them would serve one
  operator's credential to another. They are also the cheap ones, so this costs
  nothing. A test asserts no token-bearing panel is in the shared set.

- **Still open: write actions.** Retry, cancel, pause-schedule and re-drive are
  MCP-only. In a browser they need CSRF protection, confirmation and an audit
  trail of who did what — none of which the read-only console has.
- ~~The job list is not indexed for it.~~ **MEASURED AND BOUNDED.** On a
  200k-row partition holding 20 discarded jobs, listing the discarded ones is a
  parallel seq scan reading **4,634 buffers** — `id` is uuidv7 so it walks the
  primary key backwards, but nothing indexes `state` outside the claim
  predicate.
  
  No index was added. One on `(queue, state, id)` would make every insert and
  every transition pay, forever, for a dashboard that is usually not being
  watched, and the pathological case is specifically "rare state, huge
  partition" rather than the common one. Instead the query carries a
  `statement_timeout` — because the console re-runs every panel once a second,
  so a slow query is not a slow page, it is permanent load — and the panel
  renders the timeout with an explanation rather than an empty table that reads
  as "no jobs".

## MCP control plane

The actual differentiator. Nothing else in this space has it.

### Implementation: `mcp-host` 0.3.2 (owner's crate)

`~/Projects/ecosystem/mcp/mcphost-rs`. Not `rmcp` — see `14-rust-stack.md`.

This is not a neutral choice between equivalent crates; it collapses most of
this section's integration work:

- **rama-backed HTTP transport**, so MCP shares the framework with the API, SSE
  and dashboard rather than standing up a second HTTP stack
- **already built on `breaker-machines`, `chrono-machines`,
  `throttle-machines`** — the same crates the job core uses, so a tool call and
  a job execution are protected by one set of primitives with one set of
  semantics, not two
- MCP **2025-11-25**, tools/resources/prompts, elicitation, tasks,
  bidirectional, middleware chain, session lifecycle state machine
- **per-session contextual visibility** for tools/resources/prompts — directly
  useful for the guarded write tools below: an operator session can see
  `job_retry` while a read-only session cannot, enforced by the transport rather
  than by our own checks
- STDIO and in-process transports come free, so the MCP surface is testable
  without a listener and usable from a local CLI

### Why it matters

Debugging a job queue is a conversational task: "why does this keep failing",
"what changed at 3am", "retry everything that hit the payment API". Those are
natural MCP tool calls, and answering them today means a human clicking through
a web UI or writing ad-hoc SQL.

We have the muscle memory from ActionMCP in CommonShare.

### Tool surface (first pass)

Read:
- `queue_status` — depth, throughput, oldest available job, per queue
- `job_inspect` — full record, transition history, attempts, last error
- `job_search` — by task, state, error substring, time window, tenant
- `failure_summary` — cluster recent failures by error signature
- `worker_status` — heartbeats, what each worker is currently executing
- `throughput_trend` — enqueue vs completion rate, to answer "are we falling behind"

Write (guarded):
- `job_retry` — single or matched set
- `job_cancel` / `job_discard`
- `queue_pause` / `queue_resume`
- `throttle_adjust` — change a rate limit at runtime

### Design notes

- Write tools need explicit confirmation semantics. "Retry everything matching
  X" against 100k jobs should state the count and require confirmation, not just
  fire.
- `jsonb` args (see `05-polyglot-protocol.md`) make `job_search` genuinely
  useful — opaque `bytea` payloads would make the MCP surface much weaker. This
  is a concrete reason to prefer `jsonb`.
- Same auth boundary as the HTTP API.
- Read tools are the 80% case and are safe; ship those first.

### Positioning

Everything else in Stampede is copyable in a quarter by a motivated competitor.
A well-designed MCP control plane is not, because it requires the schema, the
resilience crates and the daemon to have been designed for introspection from
the start.

## The write gate, verified

The MCP surface had **no tests at all** — eleven tools, four of which change
state, behind a single `allow_writes` boolean. Verified against the real stdio
binary, driving JSON-RPC by hand:

| | tools advertised | `job_cancel` | job afterwards |
|---|---|---|---|
| `STAMPEDE_MCP_ALLOW_WRITES=0` | 7 | `Tool not found` | `available` |
| `STAMPEDE_MCP_ALLOW_WRITES=1` | 11 | `job … cancelled` | `cancelled` |

The gate is correct, and the write tools are not merely hidden from
`tools/list` — calling one by name on a read-only server is refused, which is
the part that matters, since a hidden tool an attacker guesses is not gated at
all.

It is now also tested, because the risk was never that the gate is wrong today.
`mount()` registers tools in two blocks that look identical apart from a
`Some(visibility)` argument, so adding a new write tool to the wrong one is a
one-line mistake with no symptom: the tool works, and it works for everybody.
The test writes out the expected write set rather than deriving it, so a new
write tool fails until someone says which side it belongs on, and it asserts
the two routers differ by *exactly* that set — which also catches the reverse
mistake of gating a read tool, whose only symptom is that it quietly stops
being advertised.