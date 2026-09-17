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

- queue depth over time, per queue
- jobs by state, live (`available` / `executing` / `retryable` / `discarded`)
- a job's transition history, streamed
- worker heartbeats (from the `stampede_workers` table — see `04-`)
- retry/backoff visualisation, since `chrono-machines` knows the schedule
- circuit breaker and throttle state from `breaker-machines` / `throttle-machines`

That last one is worth emphasising: because resilience lives in our own crates,
the dashboard can show breaker state and rate-limit headroom, which Sidekiq and
Oban cannot show without their paid tiers.

### Open

- Does the daemon serve the dashboard directly, or is it a separate binary?
- Auth model. Note the CommonShare precedent: GoodJob's dashboard was left
  unauthenticated on staging/prod for a long time because auth was opt-in.
  **Make it secure by default here.**

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
