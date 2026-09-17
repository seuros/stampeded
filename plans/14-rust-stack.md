# The Rust stack — `rama`, and what Underway teaches

## DECIDED: rama. (2026-09-17, by the project owner.)

**Stampede is built on `rama`.** This is settled and not open for re-litigation
by a future reader or a future agent.

The rest of this file was written as a decision memo weighing rama against axum.
That question was mine, not the owner's — I recorded it in
`08-reference-survey.md` and briefed an agent to answer it in memo form. The
owner is rama's #3 contributor, credited in 0.4.0; the maturity, churn and
bus-factor arguments below are steerable by him in a way they would not be for
an outside adopter, which is the assumption the memo was written under and got
wrong.

**Keep the memo for its engineering content, not its verdict.** Two findings
survive the decision and become work items rather than objections:

1. ~~MCP on rama needs a design.~~ **NOT AN ISSUE — the memo evaluated the
   wrong crate.** It weighed `rmcp` (hyperium-native, hence the type-system
   collision with rama) and never established whether that was the MCP crate we
   would actually use. It is not.

   **Stampede uses `mcp-host` 0.3.2** — `~/Projects/ecosystem/mcp/mcphost-rs`,
   owner's crate:

   - MCP protocol **2025-11-25**, full JSON-RPC 2.0 with version negotiation
   - tools, resources, prompts; elicitation; tasks; bidirectional
     (server→client `roots/list`, `sampling/createMessage`)
   - **rama-backed HTTP transport**, feature-gated (`http = ["dep:rama",
     "dep:rama-http-backend"]`, `Cargo.toml:27,64-65`), plus STDIO and
     in-process
   - per-session contextual visibility for tools/resources/prompts
   - **already built on `breaker-machines`, `chrono-machines` and
     `throttle-machines`**

   So the entire §§2-3 argument — rama's hyperium fork colliding with a
   hyperium-native MCP crate — is moot. There is no bridging, no second HTTP
   stack, no hand-rolling. MCP and the job core share both the HTTP framework
   *and* three of the four resilience crates.

   **Correction to the memo below: rama is at 0.4.0, not 0.5.0.** Verified on
   crates.io — `max_version: 0.4.0`, `newest_version: 0.4.0`; the registry holds
   `0.4.0, 0.3.0` and 0.3.0 pre-releases, and **0.5.0 has never been published**.
   The memo read the version out of rama's git tree, which carries the
   unreleased in-development number, and reported it as shipped.

   So `mcp-host`'s `rama = "0.4.0"` pin is **the current release**, not a lag.
   There is no version alignment task. Anywhere the memo below reasons about
   0.5.0 — including its maturity and churn arguments — read 0.4.0 and discount
   accordingly.
2. **Datastar on rama is already first-class** — typed events, `include_str!`'d
   datastar.js, and a 5,200-line working dashboard in `rama-cli`. This is a real
   advantage for `06-`'s no-Node-toolchain goal and an argument *for* rama that
   the revised memo undersold. Note `Event::serialize()` is `pub(super)`.

Underway's connection-pool lesson (§5) is independent of this decision and still
applies in full.

---

*Original memo follows. Its recommendation is superseded; its measurements,
citations and Underway analysis are not.*

> **Revision note.** The first draft of this memo recommended **(c)** — rama at
> the HTTP edge only — and named its own flip condition: *"if the `datastar-rust`
> axum adapter is strong, (b) becomes the better call"* (old §1, old Still-Open
> item 1, flagged "highest-value item on this list"). That draft was written with
> no network access. The condition has now been checked and **is met** (§1.5),
> and two further facts landed with it: rama forks the hyperium *type system*,
> not just hyper (§1.4), and the official MCP SDK is hyperium-native (§2.1).
> The recommendation moves to (b) on the project's own stated criterion. The
> prior draft's in-tree findings are preserved throughout; only the conclusion
> changed.

Provenance is marked **[V]** (read at the cited `file:line`, or queried from
crates.io / the GitHub API on 2026-09-17) and **[I]** (reasoning or recall).

---

## 1. What `rama` actually is

### 1.1 Facts **[V]**

| fact | source |
|---|---|
| Version in tree is **0.5.0** (unreleased); latest **published** is 0.4.0 | `rama/Cargo.toml:85`; crates.io |
| 0.4.0 released 2026-08-19 | `rama/CHANGELOG.md:8-10`, crates.io |
| HEAD dated 2026-09-16, pushed 2026-09-17 — actively developed | `git log -1`, GitHub API |
| Dual MIT / Apache-2.0 — safe to read | `rama/LICENSE-APACHE`, `LICENSE-MIT` |
| **818k lines of Rust**, 47-member workspace | `find . -name '*.rs' \| xargs wc -l`; `Cargo.toml:26-77` |
| Single code owner: `* @glendc` | `rama/CODEOWNERS:1` |
| 51 contributors; GlenDC 1,956 commits, #2 is 132, **#3 is `seuros` at 27** | GitHub API |
| 1,209 ★, 103 forks, 58 open issues, 10 watchers | GitHub API |
| 72,633 downloads total / 23,625 recent | crates.io |
| MSRV **1.96.0**, edition 2024 | `rama/Cargo.toml:87,99` |
| 721 packages in the workspace lockfile (all features; **not** an app's tree) | `rama/Cargo.lock` |
| docs are **good**: 64-page book (6,749 lines), 91 tested examples | `docs/book/src/`, `examples/src/*.rs` |

### 1.2 The core abstraction **[V]**

`rama_core::Service<Input>` — `async fn serve(&self, input) -> Result<Output, Error>`
(`rama-core/src/service/svc.rs:66-82`). Three deliberate departures from Tower:

- `&self`, not `&mut self`. Services are shared, not cloned-per-call.
- **No `poll_ready`.** No backpressure channel in the trait; limiting is done
  with `Limit<S, P>` + a `Policy` (`rama-core/src/layer/limit/mod.rs:26-29`,
  policies in `layer/limit/policy/{concurrent,rate}.rs`).
- No separate `Context` argument. Dynamic state travels in `Extensions` attached
  to the input (`rama-core/src/extensions.rs:1-31`, `rama-core/src/svc_input.rs:11-27`).

This is a genuine improvement on tower's readiness protocol, which exists to
serve client-side load balancing — a problem Stampede does not have.
`Service<Input>` is fully generic over input; nothing forces it to be HTTP. A
job could in principle be an `Input`. That is the honest steelman for (a).

`rama-core` itself is lean: bytes, futures, tokio, tracing, serde, parking_lot,
ahash, tokio-graceful/-stream/-util (`rama-core/Cargo.toml:57-79`). No hyper, no
TLS, no HTTP. `std` is a feature; the crate is `no_std`-capable.

### 1.3 Proxy framework, or general service framework? **[V]**

Both claims are true; the mass is lopsided. LOC by crate:

| crate | LOC | Stampede needs it? |
|---|---|---|
| `rama-http` | 98,504 | partly (router, layers, SSE) |
| `rama-net` | 67,985 | no |
| `rama-http-types` | 38,276 | partly (SSE + Datastar types) |
| `rama-http-core` | 34,246 | **vendored hyper + h2 fork** |
| `rama-core` | 25,021 | maybe |
| `rama-utils` | 15,963 | incidental |
| `rama-tls` | 9,107 | no |
| everything else (QUIC, SOCKS5, ICAP, FastCGI, PAC, JS, UA, DNS, HAProxy, gRPC, ttRPC, Apple NE/XPC) | ~500k | no |

The general-purpose service core is ~3% of the tree. The rest is proxy and
network-protocol surface: MITM, TLS termination, JA3/JA4/JA4H/Akamai
fingerprinting, User-Agent emulation with embedded browser profiles, PAC
evaluation in an embedded JS runtime, system-proxy discovery, HAR capture. This
is a **proxy framework with a good general service core**, not a general service
framework that also does proxies.

### 1.4 The fork is of the *type system*, not just the implementation **[V]**

This is the fact the first draft missed, and it is load-bearing.

`rama-http-types/Cargo.toml` has **no dependency on the `http` crate**. It
defines its own `Request`, `Response`, `HeaderMap`, `HeaderName`, `HeaderValue`,
`Method`, `StatusCode` (`rama-http-types/src/lib.rs:41-53`) and its own `Uri`
(via `rama-net`), plus forked `http_body` / `http_body_util`.

`docs/thirdparty/fork/README.md:1-58` lists the permanent forks: `hyper`, `h2`,
`hyper-util`, `headers`, `tonic`, `http`, `http-body`, `http-body-util`, plus
`quinn`. These are "sync forks" — tracked against upstream by hand.

Two consequences:

- `rama-http-hyperium/src/lib.rs:1-31` is the opt-in bridge, and its own doc
  comment is the tell: the conversions are **fallible by design** — an invalid
  method/status, unrepresentable URI, or invalid header surfaces as an error
  rather than being coerced.
- `rama-tower` bridges `Service`/`Layer` *shapes* (`rama-tower/src/lib.rs:6-31`)
  but **not types**. A `tower-http` layer over `http::Request` adapted into a
  rama stack is a rama `Service<http::Request>`, not a
  `Service<rama::Request>`; splicing it into a rama HTTP stack still needs the
  hyperium bridge.

So "Tower interop exists" is true and much weaker than it sounds. Adopting rama
at the edge means every `http`-ecosystem crate — auth, OpenAPI, MCP, sessions,
metrics exporters — arrives through a hand-maintained fallible bridge, or not at
all. That is a per-ecosystem integration tax, paid forever, and it rhymes badly
with `05-polyglot-protocol.md:8-14`, which exists to *avoid* exactly this shape
of cost.

### 1.5 The Datastar question, now closed **[V]**

The first draft's decisive open item. Both halves check out against rama, and
against it:

**In rama's favour, verified:**

1. `rama-http-types/src/body/sse/datastar/` — typed events (`PatchElements`,
   `PatchSignals`, `ExecuteScript`), with a `ReadSignals` extractor
   (`rama-http/src/service/web/endpoint/extract/datastar.rs`) and
   `DatastarScript`, which `include_str!`s datastar.js **1.0.3** straight into
   the binary (`rama-http/src/service/web/endpoint/response/datastar.rs:16-22`).
   That delivers `06-dashboard-and-mcp.md`'s "no Node toolchain in the
   deployment" verifiably rather than aspirationally.
2. **A working Datastar dashboard in-repo: 7,681 lines**,
   `rama-cli/src/cmd/serve/proxy/dashboard/` — a live traffic inspector on the
   same Router + SSE + Datastar stack, with render coalescing
   (`dashboard/live.rs:57-68`), a `Semaphore` cap on concurrent event streams
   (`live.rs:33`), and `KeepAliveStream` (`live.rs:40`). Stampede's dashboard is
   structurally the same program. A template, not just an example.

**Against, verified, and this is what flips the memo:**

1. **The official SDK is real and axum-native.** crates.io `datastar` **0.4.0**,
   95,437 downloads total / 48,755 recent, repo `starfederation/datastar-rust`,
   declared keywords `axum, rocket, warp`. Its stated design principle: *"Rust
   web frameworks own SSE stream lifecycle and backpressure, so this SDK
   intentionally returns framework-native events instead of providing a
   `ServerSentEventGenerator`."* It ships an `axum-watch` example driving one SSE
   response from three `tokio::sync::watch` channels — precisely the dashboard
   shape in `06-`. Recent-download ratio (51%) says it is on an adoption curve,
   not abandoned.
2. **rama's version cannot be cherry-picked.** `Event::serialize()` is
   `pub(super)` (`rama-http-types/src/body/sse/event.rs:74`). rama's Datastar
   types emit bytes only through rama's own `Body`. There is no "take the good
   part, serve it with axum" path. It is all-or-nothing at the HTTP boundary.

Net: advantage 1 collapses to convenience — the embedded-JS trick is one
`include_str!` we can write ourselves in an afternoon. Advantage 2, the in-repo
dashboard, survives as a **reading reference**: we can study
`rama-cli/.../dashboard/` for coalescing and stream-cap design without depending
on rama at all. Under the flip condition the first draft set for itself, that is
not enough to carry the framework.

### 1.6 What else it does that axum/tower/hyper do not

`rama::graceful` — but `rama-core/src/graceful.rs:3-6` is **six lines**, a pure
`pub use ::tokio_graceful::{...}`. **[V]** Depend on `tokio-graceful` directly;
100% of the value, zero framework. That re-export is the tell for this whole
memo: the one piece of rama-core that maps cleanly onto a job daemon is already
a separate crate.

Everything else on the differentiator list (fingerprinting, MITM, protocol
bridging, PAC, UA emulation) is irrelevant to a job processor.

---

## 2. Per-component: helps, neutral, or overhead

| component | verdict | why |
|---|---|---|
| **HTTP + SSE API** (`POST /jobs`, `GET /jobs/:id/events`) | **neutral** | Router, typed extractors, `Sse`, `KeepAlive` all present and axum-shaped (`examples/src/http_web_router.rs:69-95`). axum does the same on ~4 routes. No decisive advantage either way. |
| **Datastar dashboard** | **helps → now neutral** | Was the decisive row in the first draft. §1.5 dissolves it: the official SDK is axum-native, and rama's serializer is private so there is no middle path. |
| **MCP endpoint** | **overhead — now the deciding row** | See §2.1. |
| **Worker supervisor** | **neutral — illusory help** | `graceful` is a 6-line re-export of `tokio-graceful`. The rest is `JoinSet` + `CancellationToken`. |
| **Claim / dispatch loop** | **overhead** | Hold a connection, run one statement, drive a state machine, spawn an executor. `Service<Input>` adds vocabulary; `Extensions`-based dynamic state is strictly worse than a concrete typed struct here. Nothing in rama touches fencing, lease expiry, or attempt accounting. |
| **Retry / rate limit / breaker** | **overhead — active conflict** | rama ships `rama_utils::backoff::ExponentialBackoff` (`rama-utils/src/backoff/exponential.rs:17`), `layer::limit::policy::{RatePolicy, ConcurrentPolicy}` (`policy/rate.rs:57`, `policy/concurrent.rs:33`), `http::layer::retry` (`rama-http/src/layer/retry/managed.rs:39-66`), and bandwidth token buckets (`rama-net/src/stream/layer/throttle/`). Direct duplicates of `chrono-machines` and `throttle-machines`. Adopting rama means two vocabularies for the same concerns and dilutes the `02-crate-family.md` thesis. |

### 2.1 Why MCP is now the deciding row **[V]**

`00-vision.md:36-40` is unambiguous: everything else in Stampede is copyable in a
quarter; the MCP control plane is not. So the differentiator sits on whichever
crate serves MCP.

The official Rust SDK, `rmcp` **3.4.0** (`modelcontextprotocol/rust-sdk`), has
**27,052,775 downloads total / 13,936,339 recent** — three orders of magnitude
past rama — and declares `dep:hyper`, `dep:http`, `dep:bytes` plus a `tower`
feature, with `transport-streamable-http-server` and `server-side-http`. It is
hyperium-native.

On rama that is one of three bad options: (i) bridge every MCP request and
response through `rama-http-hyperium`'s fallible conversions; (ii) stand up a
second HTTP stack on a second listener — two HTTP/1+2 implementations, two type
systems, two CVE surfaces in one binary; (iii) hand-roll MCP. Option (iii)
deserves naming because it is the tempting one and it is a trap: MCP revs fast
(the CommonShare notes have `2025-06-18` and `2025-11-25` both live), and
hand-rolling it is the per-ecosystem client-library tax of
`05-polyglot-protocol.md:8-14` — paid by us instead of by users, which is worse,
not better.

This closes old Still-Open item 3.

---

## 3. What it would cost

| axis | assessment |
|---|---|
| **Maturity** | 0.x with a breaking-change train. The 0.3→0.4 changelog carries a dedicated **"Breaking API Changes"** section (~58 lines, 13 headline items, self-described as "not an exhaustive symbol-by-symbol inventory") plus a **"Removed"** section (~25 lines) in one cycle (`CHANGELOG.md:139,318`). 0.3.0 shipped 2026-07-07 and 0.4.0 six weeks later; 0.5.0 is already in tree. Expect to pay that every release. **[V]** |
| **Adoption** | 72,633 lifetime downloads vs axum's 472,772,128 (116M recent). Roughly a 6,500× gap. Not dispositive on its own — it is dispositive in combination with the forked type system, because obscurity plus a fork means nobody else has hit your integration bug first. **[V]** |
| **MSRV** | **1.96.0 / edition 2024** — effectively "latest stable, no MSRV policy". Stampede inherits that floor wherever rama is linked. **[V]** |
| **Dependency weight** | 721 locked packages workspace-wide (upper bound; feature-gated builds are far smaller — features are granular, `Cargo.toml:79-160`). apalis's whole workspace locks 399 for comparison. **[V]** |
| **Docs** | **Better than most 0.x and better than we need**: 64-page book, 91 tested examples, full rustdoc plus edge docs. This is not a weakness and should not be argued as one. **[V]** |
| **Bus factor** | `CODEOWNERS: * @glendc`; GlenDC has 1,956 of ~2,200 commits (~88%); #2 contributor has 132. Commercial backing (Plabayo, ramaproxy.com) and 51 total contributors are mitigants, not a fix. **Note:** `seuros` is the **#3 contributor at 27 commits** and is credited by name in the 0.4.0 release notes — real influence exists. See §7.2, where this is the counterargument's spine. **[V]** |
| **Vendored HTTP stack** | `rama-http-core` is a **34,246-line fork of hyper + h2** (`rama-http-core/Cargo.toml`). Stampede would inherit a non-upstream HTTP/1+2 implementation and its CVE-response latency, on an internet-facing surface. With axum you get hyperium's hyper: largest deployed base, fastest security response in the Rust HTTP ecosystem. **This is a risk that cannot be discharged by sending a PR.** **[V]** for the fork; **[I]** for relative response speed. |
| **Ecosystem tax** | §1.4. The fork is of the type system, so `http`-ecosystem crates need bridging or reimplementation, permanently. **[V]** |
| **Abstraction leakage** | Contained *if* rama is confined to one crate. If rama's `Service`/`Extensions` reaches the claim path, the job lifecycle acquires a second, untyped state channel next to `state-machines` — the same two-sources-of-truth failure `03-postgres18.md:81-86` already rejects for triggers. **[I]** |
| **Backpressure model** | No `poll_ready` means rama cannot express Tower-style demand-driven backpressure. Fine for an HTTP edge. For a dispatcher that must stop claiming when executors saturate, it is a mismatch — and apalis (§5) shows the Tower model genuinely earns its keep there. **[V]** for the trait; **[I]** for the consequence. |

---

## 4. Underway — the closest prior art on our exact stack

`reference/postgres/underway`, v0.2.0 + unreleased, MIT/Apache-2.0, 13,866 lines,
172 ★ / 17 open issues, single primary author (maxcountryman, of
`tower-sessions` / `axum-login`). Safe to read. **The most useful thing in the
reference set after Oban.**

### 4.1 What it builds on **[V]**

`sqlx 0.9` (postgres, runtime-tokio, tls-rustls-ring, uuid, json) + `tokio` with
`features = ["full"]` and an admitted `TODO: "full" shouldn't be required`
(`Cargo.toml:18-26`), `jiff` + `jiff-cron` for time and schedules, `ulid`/`uuid`
for ids, `num_cpus`, `tokio-util`, `rand`, `thiserror`, `tracing`. **No tower, no
deadpool, no bb8, no axum** — sqlx's own `PgPool` throughout. `.sqlx/` holds
~100 offline query-metadata files for `query!`-macro compilation.

### 4.2 Structure **[V]**

`Runtime` spawns a `JoinSet` of `{Worker, Scheduler, ActivityWorker?}` sharing one
`CancellationToken` (`src/runtime.rs:208-213`), with `RuntimeHandle::shutdown`
joining them (`src/runtime.rs:112`).

**Worker loop** (`src/worker.rs:578-640`) — this part is right and we should
copy it:

- outer `'reconnect` loop owning a `PgListener` on `task_change` + a shutdown
  channel, reconnecting with backoff on listener loss;
- inner `tokio::select!` over four arms: `JoinSet` reaping, shutdown token,
  LISTEN notification, and a **polling interval as fallback**.

LISTEN/NOTIFY as the fast path with polling as the safety net is the correct
shape. A pure-NOTIFY design silently stalls on a dropped listener; a pure-poll
design trades latency for nothing.

**Concurrency** is a `Semaphore(num_cpus::get())` (`worker.rs:251`, `:581`).
Subtlety worth noting: `trigger_task_processing` (`worker.rs:706-738`) takes one
permit and then loops `process_next_task` until the queue drains before releasing.
Permits gate *drain loops*, not in-flight jobs; each loop runs one job at a time,
so effective concurrency still equals the permit count.

**Shutdown** waits on the JoinSet with a timeout equal to the task timeout
(`worker.rs:653`).

**Claim** (`src/queue.rs:1051-1152`) is one statement with four CTEs:

1. `available_task` — `order by priority desc, run_at, created_at, id
   limit 1 for update skip locked` (`queue.rs:1070-1076`);
2. `claimed` — the UPDATE, bumping `attempt_count`, setting
   `lease_expires_at = now() + t.heartbeat`;
3. `finalized_previous_attempt` — closes the prior attempt row as failed;
4. `upsert_current_attempt` — writes the new `task_attempt` audit row.

**The single best idea in the file:** lease reclaim is folded into the *same*
predicate as fresh work (`queue.rs:1060-1068`) — a row is claimable if it is
`pending and run_at <= now()` **or** `in_progress and lease_expires_at <= now()
and attempt_count < max_attempts`. Orphan recovery costs zero extra queries and
zero extra reaper process. `03-postgres18.md:67-71` notes PG18 does not solve
orphan recovery; this is the cheapest application-level answer, and it composes
with `RETURNING OLD./NEW.` rather than competing with it.

**Also worth copying:** the separate `underway.task_attempt` table (one row per
attempt, with started/completed/error) is a better substrate for `job_inspect`
and `failure_summary` (`06-`) than a single errors array column. Oban has no
equivalent.

### 4.3 What we would do differently **[V]** throughout

| # | underway does | problem | Stampede |
|---|---|---|---|
| 1 | **One pooled connection pinned per in-flight job for the job's entire duration.** `pool.acquire()` at `worker.rs:810`, `conn.begin()` at `:820`, held until `tx.commit()` at `:886`. The user's `execute` even receives a savepoint (`:859`). | Connection count scales with *concurrency × job duration*, not query volume. A 30-second HTTP-calling job holds a Postgres connection for 30 seconds doing nothing. | Claim commits immediately; execution holds **no** DB connection; completion is a separate short transaction. **We give up "the job body runs in the queue's transaction" — the one thing underway offers that we will not. Document that loudly.** See §6. |
| 2 | Heartbeat task borrows a **second** pool connection each beat (`worker.rs:838-851`), deliberately outside the tx. | Doubles peak connection pressure exactly when the pool is most contended. | One heartbeat writer per daemon, batching all in-flight job ids into a single UPDATE on one connection. |
| 3 | `pg_try_advisory_xact_lock(hashtext($1))` for concurrency keys (`queue.rs:1864-1873`). | `hashtext` is **32-bit**. Unrelated concurrency keys collide and silently serialize; ~50% collision probability around 77k distinct keys **[I]** (birthday bound, not measured). | Use a 64-bit key, or lean on the partial unique index underway already has (`migrations/20240921151751_0.sql`, `idx_task_concurrency_key_unique ... where state in ('pending','in_progress')`) — the better mechanism, which makes the advisory lock largely redundant. |
| 4 | `pg_notify` from a **FOR EACH ROW** trigger (`migrations/20241024174106_1.sql:19-21`; repeated for activities at `20260209120000_6.sql:70`), payload = queue name only, filtered **in the worker** (`worker.rs:684-693`). | Bulk-enqueueing 100k jobs emits 100k notifications to every listening worker of every queue, which then race for the same rows. A notify storm on the exact operation a job queue must handle well — **the named failure mode of this project**. | Statement-level trigger, or drop the trigger and `NOTIFY` explicitly from the enqueue helper; encode the queue in the *channel name* so Postgres filters; debounce/coalesce in the daemon. See `12-wakeup-and-notify.md`. |
| 5 | Scheduler **exits permanently** if it loses the leader-lock race (`scheduler.rs:357-359`, `"Scheduler could not acquire lock, exiting"` then `return Ok(())`). | No standby. When the leader dies, the losers already quit; cron stops until something restarts them. | Retry-with-backoff on lock acquisition; a loser is a standby, not a corpse. `chrono-machines` already has the backoff. |
| 6 | No `MATERIALIZED` anywhere (zero matches across `src/` and `migrations/`). | Underway dodges Oban's optimisation-fence bug (`README.md:34-38`, `basic.ex:119`) only because `FOR UPDATE` happens to block CTE inlining, and because `limit 1` makes overrun invisible. Relying on that implicitly is exactly the "someone will simplify it back into the bug" hazard. | PG18 `RETURNING OLD./NEW.` removes the subquery entirely; the reasoning goes in a schema comment either way. |
| 7 | `limit 1` per dequeue (`queue.rs:1075`), and `fail_stale_exhausted_tasks` runs on **every** dequeue (`queue.rs:1044`) — a locking scan of expired-lease rows before every single claim. | One round trip per job, with janitor work on the hot path under a lock. | Batch claims via `RETURNING OLD./NEW.` + the fence. Reaper gets its own cadence and its own connection. |
| 8 | Client-generated ids: `TaskId(Ulid::new().into())` (`task.rs:108`). | Fine, but redundant on PG18. | `uuidv7()` server-side (`03-postgres18.md:11-21`). |
| 9 | Retry policy stored per row as a Postgres **composite type** — note `(retry_policy).max_attempts` (`queue.rs:1067`). Jitter is *equal* jitter, `target*(1-j) + rand(0..=target*j)`, default `j=0.5` (`src/task/retry_policy.rs:46-47,58`). | Composites are painful to evolve (`ALTER TYPE` on a hot table). Equal jitter is weaker than full. | Policy table + FK, or plain columns with a virtual generated `is_retryable` (`03-postgres18.md:39-49`). Full jitter from `chrono-machines` (`02-crate-family.md:40-45`). |
| 10 | Filters the hot task table on `input->>'workflow_run_id' = $1` (`workflow.rs:888`). | `jsonb` args are right for `05-`/`06-`, but unindexed `input->>...` predicates in operational queries are a silent scan. | Expression indexes designed in, not discovered. Concrete support for the `jsonb`-over-`bytea` call in `09-`. |
| 11 | Never configures the pool. Zero `PgPoolOptions` / `max_connections` references in `src/` or `examples/`. | The connection budget is the user's problem and is documented nowhere. | Declared budget in config, asserted at startup, exported to the dashboard. |
| 12 | Has become a **workflow engine**: `src/workflow.rs` is 3,278 lines (typed steps, `Transition`, replay-safe `InvokeActivity`) on top of a 4,226-line queue; the README now says "durable background workflows" (`README.md:23`). | `00-vision.md:43-46` declares "not a workflow engine". Underway is live evidence of that boundary eroding under its own momentum — the workflow layer is now as large as the queue. | Cite this when the pressure arrives. **The non-goal needs an owner, not just a sentence.** |

**Churn warning [V]:** `CHANGELOG.md:1-60` is almost entirely `Breaking:` from
0.1.0 onward. `sqlx` is in underway's public API (`PgPool`), so a sqlx major
forces a lockstep major on every consumer (`CHANGELOG.md:3-5`). If Stampede
exposes a pool type publicly, we inherit that. Consider not exposing it.

---

## 5. apalis — brief

`reference/other/apalis`, **1.0.0-rc.10** (2026-09-15), MIT, edition 2024, MSRV
1.85, `unsafe_code = "forbid"` (`Cargo.toml:13`), 399 locked packages.
**1,154,995 downloads total / 307,258 recent** — by a wide margin the
most-adopted Rust job framework, ~16× underway. In release-candidate across at
least rc.8 → rc.10 (rc.8 and rc.9 both 2026-05-06, rc.10 four months later). **[V]**

**Architecture. [V]** `apalis-core` is runtime-agnostic and depends on
`tower-service` + `tower-layer` 0.3.3 only, not `tower` itself
(`apalis-core/Cargo.toml:29-30`; full `tower` is a dev-dependency, `:78`).
`Backend` (`apalis-core/src/backend/mod.rs:113-142`) is a Stream-shaped,
poll-based source of tasks with its own `poll_ready`/`poll_next` and explicit
laziness: *"a backend should only produce a task when one is actually available,
not busy-poll its source"* (`mod.rs:138-142`). Handlers are plain async fns
adapted into `tower::Service<Task<Args>>`
(`apalis-core/src/worker/service.rs:42,82`) with axum-style `Data<T>` injection.

**Gets right — the strongest argument for a Tower-shaped core. [V]** The
execution path reuses stock Tower middleware unmodified:
`tower::limit::{ConcurrencyLimitLayer, GlobalConcurrencyLimitLayer,
RateLimitLayer}` re-exported at `apalis/src/layers/mod.rs:24-28`, and
`tower::timeout::TimeoutLayer` at `:39`, plus retry, catch-panic, tracing,
prometheus, sentry, otel as layers. That is direct evidence the Service/Layer
model is **not** purely a network-middleware idea — it applies cleanly to job
execution.

**Gets wrong. [V]** The storage backends live in **separate repositories**
(`apalis-postgres`, `apalis-redis`, `apalis-sqlite` per the README crate table),
so the interesting part — Postgres claiming — is not in this tree at all. That is
the design consequence, not an accident: `Backend`-as-abstract-stream pushes
fencing, lease expiry and attempt accounting *out* of the framework and into each
backend, where every backend re-derives and re-breaks them. Still `1.0.0-rc`
after a long run.

**For us:** Stampede's Postgres-only stance is the opposite bet and is the right
one for correctness. But note what apalis does to the rama argument — its
evidence favours **Tower**, whose middleware ecosystem Stampede gets from `tower`
directly, and whose `poll_ready` backpressure rama deliberately dropped. It is
not evidence for rama.

**Still worth a second pass** before the schema freezes: `apalis-postgres` is not
cloned, and its claim SQL, connection-hold behaviour during execution (the §4.3.1
trap), and orphan reclamation are the three things we would most want to compare
against underway.

---

## 6. Connection pooling

### 6.1 The landscape **[V]**

| project / option | pool | config |
|---|---|---|
| underway | sqlx `PgPool` | none — caller's problem (`src/`, `examples/`: zero `PgPoolOptions`) |
| apalis-sql | sqlx | out of tree **[I]** |
| sqlxmq (0.6.0, last release 2025-05-25, 187k downloads) | sqlx | effectively dormant; not worth a read |
| rama-cli | `bb8::Pool<PostgresConnectionManager<NoTls>>` over tokio-postgres | `connection_timeout(5s)`, **no `max_size`** → bb8 default 10 (`rama-cli/src/cmd/serve/fp/storage/postgres.rs:8-19`) |
| `deadpool-postgres` | tokio-postgres | leaner, no compile-time query checking, fastest checkout path **[I]** |

**sqlx 0.9.0** (2026-05-21), 147M downloads, is the default answer: pure Rust,
compile-time checked queries, pool built in.

**sqlx `PoolOptions` defaults [V]** (`sqlx-core/src/pool/options.rs`):
`max_connections = 10`, `min_connections = 0`, `acquire_timeout = 30s`,
`idle_timeout = 10min`, `max_lifetime = 30min`, `test_before_acquire = true`,
`acquire_slow_threshold = 2s`. **Ten.** Underway never overrides this.

### 6.2 The trap, quantified

Underway's steady-state connection floor per process **[V]** for every line:

```
concurrency_limit (default num_cpus)      one pinned per in-flight job, worker.rs:810-886
+ 1 PgListener per Worker                 detached from pool, queue.rs:2236
+ 1 PgListener per Scheduler              scheduler.rs:365
+ 1 PgListener per ActivityWorker         activity_worker.rs:168
+ 1 held connection for scheduler lock    scheduler.rs:342-357
+ heartbeat churn                         worker.rs:849, one borrow per beat per job
```

On a 16-core box that is **~20 connections at rest**, spiking with heartbeats —
against a default pool of 10, so the pool is exhausted before the concurrency
limit is reached, and the failure mode is a 30-second `acquire_timeout` rather
than a clear error. Five such processes exhaust `max_connections=100` — the stock
dev instance in `03-postgres18.md:93-96` — before a single job is slow.

And because Postgres *is* the protocol (`05-`), the enqueueing applications share
that same instance. **Stampede's connection budget is not only Stampede's
problem.**

Note `sqlx::PgListener::connect_with(pool)` (`queue.rs:2236`) takes a connection
*from the pool* and holds it for the listener's life **[V]** for the call site,
**[I]** for sqlx's hold semantics. It is an invisible permanent subtraction from
`max_size`.

### 6.3 Design for Stampede — three classes, three budgets

1. **Claim/dispatch pool — small and fixed (2-4).** Claims are microsecond-scale
   statements. They must never queue behind job execution. Sizing this
   independently is the single most important structural difference from
   underway.
2. **Execution pool — bounded independently of worker concurrency.** A job that
   needs a transaction acquires on demand and **must not hold a connection across
   non-DB work**. Worker concurrency and connection count decouple. This is
   §4.3.1 restated as a rule.
3. **Dedicated non-pooled connections.** One LISTEN connection **per daemon**
   (fan out in-process to workers — not one per worker, as underway does), and
   one for the scheduler leader lock. Explicit `PgConnection`s outside the pool
   so they cannot starve it.

The budget is declared in config, asserted at startup against
`current_setting('max_connections')`, and exported to the dashboard and to
`worker_status` in the MCP surface (`06-`). Backpressure on `acquire_timeout`
should pause claiming and surface, not accumulate 30-second waits. A queue that
silently eats the database's connection budget is a worse outage than a slow
queue.

**pgbouncer caveat. [I]** Transaction-mode pooling breaks LISTEN/NOTIFY and
session-level advisory locks, and sqlx's default prepared statements need
disabling; transaction-scoped advisory locks and `SET LOCAL` are fine. "Put
pgbouncer in front" is therefore not a substitute for an in-daemon budget: the
listener and leader-lock connections must bypass it. Record this before someone
proposes it as the fix.

**Point in favour of `stampede_claim()` as a SQL function (`05-`):** a polyglot
worker calling it uses *its own* pool, not ours. The function form moves
connection cost to the client, where it is already budgeted. A second independent
reason to prefer the function over daemon-mediated claiming for in-cluster
workers.

---

## 7. Recommendation

### 7.1 (b) — `axum`/`tower` directly

Concretely:

- **HTTP edge, dashboard, MCP**: `axum` 0.8 / `tower` / `tower-http`, one HTTP
  stack, hyperium types throughout.
- **Dashboard events**: the official `datastar` crate, which returns
  framework-native SSE events. Read `rama-cli/.../dashboard/live.rs` for its
  coalescing and stream-cap design; copy the ideas, not the dependency. Embed
  datastar.js with our own `include_str!`.
- **MCP**: `rmcp`, on the same stack as everything else.
- **Shutdown**: `tokio-graceful` directly — the genuinely good piece of rama,
  which does not require rama.
- **Claim loop, supervisor, reaper, scheduler**: plain tokio. `tokio::select!` +
  `PgListener` + `Semaphore` + `CancellationToken` + `JoinSet`, exactly as
  underway does it (`worker.rs:578-642`), which is the right shape and owes
  nothing to any framework.
- **Resilience**: the four `*-machines` crates, unopposed. No second
  rate-limit/retry/backoff vocabulary in the binary.

**Why:**

1. **rama's unique value sits at layers Stampede does not operate at, and its
   unique cost lands on the one component that matters most.** The fork is of
   the hyperium *type system* (§1.4), and the MCP SDK — carrying Stampede's only
   non-copyable differentiator (`00-vision.md:36-40`) — is hyperium-native
   (§2.1). That is the collision, and it is head-on.
2. **The Datastar advantage did not survive contact with the network (§1.5).**
   The official SDK is axum-native by explicit design, and rama's own Datastar
   types cannot be cherry-picked because `Event::serialize()` is `pub(super)`.
   The first draft named this as its flip condition; the condition is met.
3. **Everything rama offers below the edge, Stampede already owns** — published,
   in use, and the thesis of `02-crate-family.md`. Routing through rama means two
   vocabularies for backoff, rate limiting and concurrency.
4. **The re-export tells the story.** `rama::graceful` is six lines of
   `pub use ::tokio_graceful`. The part of rama that fits a job daemon is already
   a separate crate. Take the crate.
5. **Asymmetric reversibility.** Starting on axum and moving the edge to rama
   later is a contained rewrite of ~6 handlers. Starting on rama and discovering
   the bridge tax after the MCP surface is built is not.

### 7.2 The strongest counterargument to my own recommendation

**The bus factor is not an external risk for this owner.** `seuros` is rama's
**#3 contributor with 27 commits** and is credited by name in the 0.4.0 release
notes. For him, rama's two real risks — one author, frequent breaking minors —
are *steerable*: an upstream break is a PR, a missing feature is a PR, and there
is a direct line to the maintainer. Standard "pick the mature crate" advice is
calibrated for people without that access. Discounting it here is arguably
miscalibration.

Stacked on that:

- **"The edge is easy, the core is hard" is how you build a job processor nobody
  can reach.** Per `00-vision.md`, Stampede's two differentiators — the MCP
  control plane and the Datastar dashboard — are **both edge features**. The
  claiming logic is a few hundred lines of SQL that Oban, River and Graphile
  have already solved and PG18 makes easier; it is the *least* differentiated
  part of the system. If the edge is where the product value and the code volume
  actually live, "use the weaker framework for the important part" is backwards.
- **rama's `&self`, no-`poll_ready` model is a better fit than tower** for the
  long-lived, shared, stateful components Stampede actually has — a claim
  dispatcher, a throttle, a breaker — where tower's readiness protocol and
  per-call `Clone` are friction. apalis (§5) is the live experiment in what
  tower-shaped job processing costs.
- **The MCP objection dissolves if MCP gets its own listener.** A control plane
  on a separate port is normal and arguably better: different auth boundary,
  different rate limit, separately disableable. Two listeners, no bridge, no
  fallible conversions.
- **The 7,681-line in-repo Datastar dashboard is a real head start** on the
  component `06-` describes, and single-binary ACME/TLS is a genuine
  self-hosting advantage for exactly the audience that picks a Postgres-only job
  processor.

**Weighing it.** The argument is strong on product and on governance, and weak on
two specific risks. First, "MCP on its own listener" means two HTTP server
implementations, two HTTP/2 implementations and two `http` type systems in one
binary — doubled build time, doubled binary, doubled CVE surface, and a standing
"which stack does this endpoint live on?" question for every new route. Second,
a 34k-line vendored hyper/h2 fork under a single CODEOWNER is a security-response
dependency that **cannot** be discharged by sending a PR, on a surface that is
internet-facing by design. Influence over API decisions is not influence over
how fast a fork ships a CVE patch.

I hold (b) — but the margin is thin enough that this should be re-argued once,
out loud, before the first line of edge code is written.

### 7.3 Why not the others

- **(c) rama at the edge only** — the previous recommendation and still the
  defensible second choice; §7.2 is its case. Rejected on the two-HTTP-stacks
  cost and on §1.5 removing its main justification, not on principle. **If (b) is
  overruled, (c) is the right fallback — not (a).**
- **(a) build on rama wholesale** — no. It puts a 0.x forked HTTP stack under the
  claim loop and the state machine, neither of which is a network problem, and
  §3's abstraction-leakage row becomes certain rather than conditional.
- **(d) something else** — no fourth option worth the sentence. Hand-rolling on
  raw hyper for a dashboard and a handful of JSON endpoints is work without a
  payoff.

**What would flip this back:** rama reaching 1.0 with a stability commitment; or
`rmcp` (or a viable alternative) growing a rama-native transport; or Stampede's
edge acquiring a genuine transport-layer requirement — mTLS worker
authentication, protocol multiplexing on one port, or a QUIC/gRPC worker channel.
Any one of those and (c) becomes correct.

---

## Still open

1. ~~`datastar-rust` axum support quality~~ — **closed, §1.5.** Official SDK,
   0.4.0, axum-native by design. This is what moved the recommendation.
2. ~~MCP transport: `rmcp` vs hand-rolled~~ — **closed, §2.1.** `rmcp` 3.4.0,
   27M downloads, hyperium-native. Hand-rolling is the `05-` client-library tax
   paid by us.
3. rama 0.5.0's release date, and whether 0.5 → 0.6 breaks the Router /
   extractor / Datastar APIs. One more breakage cycle of evidence changes the
   cost of ever revisiting.
4. Whether the backpressure on the SSE claim-handoff path (`05-`,
   daemon-mediated claiming) needs Tower semantics. With (b) this is free; note
   it as a reason (b) is cheap rather than an open risk.
5. Does the daemon serve the dashboard, or is it a separate binary (`06-` open
   question)? Now also coupled to §7.2 — if the answer is "separate anyway", the
   rama counterargument gets stronger, not weaker.
6. **`apalis-postgres` is not cloned.** Its claim SQL, connection-hold behaviour
   and orphan reclamation are the three highest-value remaining comparisons
   against underway. Do this before the schema freezes.
7. `sqlx` `query!` macros (needs a live DB or committed `.sqlx`, ~100 files in
   underway) vs runtime `query_as` with hand-written row mapping. Affects CI and
   every contributor's first five minutes.
8. Do we expose `PgPool` in the public API at all? Underway does and pays a major
   version every sqlx major (`CHANGELOG.md:3-5`).
9. Exact connection budget numbers need a load test. The *design* must declare a
   budget before that test exists; the test then validates it.
10. pgbouncer compatibility matrix for `stampede_claim()` under transaction-mode
    pooling — specifically whether the claim function is safe there while the
    listener and leader lock bypass the pooler. Supported-in-session-mode-only,
    or shipped guidance for transaction mode with LISTEN disabled and polling
    only?
11. Whether to adopt underway's lease-folded-into-claim predicate wholesale into
    `04-job-lifecycle-and-claiming.md`. It is currently the best orphan-recovery
    answer found in any reference, and `04-` predates reading it.
12. Whether to adopt a `task_attempt`-style per-attempt history table. It is what
    makes `job_inspect` and `failure_summary` good (`06-`), and it is a
    retention/partitioning cost, still blocked on `pg_partman`
    (`03-postgres18.md:57-62`).

---

## Provenance

**Verified in-tree** (read at the cited `file:line`): rama's version, licence,
LOC distribution, MSRV, CODEOWNERS, `Service` trait shape, absence of
`poll_ready`, `rama-core` dependency list, the six-line `graceful` re-export,
**the absence of any `http` dependency in `rama-http-types` and the fork
manifest in `docs/thirdparty/fork/README.md`**, the fallible `rama-http-hyperium`
bridge, `rama-tower`'s shape-not-type interop, the Datastar module,
`DatastarScript`'s `include_str!` of datastar.js 1.0.3, **`Event::serialize()`
being `pub(super)`**, the rama-cli dashboard (7,681 lines), the vendored
`rama-http-core`, the duplicate backoff/rate/retry primitives, the changelog's
breaking-change sections, the granular feature set, rama-cli's bb8 usage; all
underway structure, SQL, line numbers, connection acquisition points, the
`hashtext` advisory lock, the row-level notify trigger and its worker-side
filtering, the scheduler exit path, the equal-jitter retry policy, the composite
retry type, the `input->>` predicate, the absence of pool configuration, and the
workflow-layer size; apalis's core dependency on `tower-service`/`tower-layer`,
the `Backend` trait, and the re-exported Tower layers at
`apalis/src/layers/mod.rs:24-28,39`.

**Verified over the network** (crates.io / GitHub API / docs.rs, 2026-09-17):
rama's stars, forks, issues, contributor distribution, published version and
download counts; axum's; `datastar` 0.4.0's existence, downloads, repo, keywords
and stated design principle; `rmcp` 3.4.0's downloads and hyperium dependencies;
`apalis`, `underway` and `sqlxmq` versions, downloads and dates; sqlx's version
and exact `PoolOptions` defaults.

**Inferred** (flagged inline): relative security-response speed of
hyperium/hyper versus a fork; the 77k-key `hashtext` collision figure (birthday
bound, not measured); `apalis-postgres`'s pool choice; sqlx `PgListener`'s
pool-hold semantics; pgbouncer's transaction-mode incompatibilities; per-app
dependency weight for a feature-gated rama build; abstraction-leakage and
backpressure consequences.
