# Changelog

## [0.16.0](https://github.com/seuros/stampeded/compare/stampeded-v0.15.0...stampeded-v0.16.0) (2026-11-26)


### ⚠ BREAKING CHANGES

* **deps:** `stampeded::vitals::Phase` is the re-export of vitals-machines 0.6's `Phase`, no longer 0.4's.

### Bug Fixes

* **deps:** state-machines 0.36 and vitals-machines 0.6 ([102a548](https://github.com/seuros/stampeded/commit/102a548576b110ce6f889cbba78fc12c6638f2d2))

## [0.15.0](https://github.com/seuros/stampeded/compare/stampeded-v0.14.0...stampeded-v0.15.0) (2026-11-26)


### Features

* **queue:** enqueue on a caller's transaction ([afcd010](https://github.com/seuros/stampeded/commit/afcd010fd46be3729b83b821dba6be466cb060a5))

## [0.14.0](https://github.com/seuros/stampeded/compare/stampeded-v0.13.0...stampeded-v0.14.0) (2026-11-24)


### Features

* **dashboard:** use AM styling and add clickable job navigation ([ed8c407](https://github.com/seuros/stampeded/commit/ed8c407398e2fa6eafb657d415ebcd568c58bea9))
* **worker:** bound tenant admission and share queue logic ([be1c87d](https://github.com/seuros/stampeded/commit/be1c87d0b5bafc8cff2f8bdce1752dd196973f8c))


### Bug Fixes

* **queue:** fence claims and make failure reporting atomic ([e05a897](https://github.com/seuros/stampeded/commit/e05a8971570b4fc739b7932378c1af3740186515))

## [0.13.0](https://github.com/seuros/stampeded/compare/stampeded-v0.12.0...stampeded-v0.13.0) (2026-11-21)


### ⚠ BREAKING CHANGES

* **worker:** stampeded_puller_options gains a trailing `const char *vitals_json` field; C callers compiled against the old header must rebuild against the new one (bindings such as Stampeded.NET need the field added).

### Features

* **worker:** host-vitals admission: stop claiming while disk or thermals are short ([e958aae](https://github.com/seuros/stampeded/commit/e958aae5867e79a1ef407396879b277e3de62dba))


### Bug Fixes

* **worker:** a vitals pause waits for in-flight claims before it is reported ([7cd8306](https://github.com/seuros/stampeded/commit/7cd8306a4bb03b767525bbf5f9c9c3d4487bfdc9))

## [0.12.0](https://github.com/seuros/stampeded/compare/stampeded-v0.11.0...stampeded-v0.12.0) (2026-11-21)


### Features

* schedule owners and per-owner declarative sync ([386d0cf](https://github.com/seuros/stampeded/commit/386d0cf2b204ba37f2d784808f1c1fb3457e4362))

## [0.11.0](https://github.com/seuros/stampeded/compare/stampeded-v0.10.0...stampeded-v0.11.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* **deps:** the jiff types in stampeded's API (Job timestamps, schedule times) are jiff 1.0's.
* **deps:** Worker::backoff takes and default_backoff returns a
* **deps:** Admission::Throttled carries `retry_after: Duration` instead of `retry_after_secs: f64`, and Fairness::admit_at and

### Bug Fixes

* **deps:** behavior-machines 0.5, circuit 0.6, throttle-machines 0.6 ([02cb02e](https://github.com/seuros/stampeded/commit/02cb02e0a1a48f31e7476dd1bd9f8cd76df096e4))
* **deps:** chrono-machines 0.13, breaker 0.27, deadline 0.6, state-machines 0.35, priority 0.7 ([66b6906](https://github.com/seuros/stampeded/commit/66b6906fae8517b4b23030de63ca67a64de0cd16))
* **deps:** jiff 1.0, jiff-sqlx 1.0 ([cb562dc](https://github.com/seuros/stampeded/commit/cb562dcae701ed6a92762aa36ae139c48fed76de))
* **deps:** mcp-host 0.28.1 ([bf8817c](https://github.com/seuros/stampeded/commit/bf8817c6c41861cfc91fd9ab57e8591849932797))

## [0.10.0](https://github.com/seuros/stampeded/compare/stampeded-v0.9.0...stampeded-v0.10.0) (2026-11-09)


### Features

* **http:** serve_on takes a listener the caller already bound ([8b2a7fd](https://github.com/seuros/stampeded/commit/8b2a7fd9a94a21ae26bf93fb897819d4e0833319))

## [0.9.0](https://github.com/seuros/stampeded/compare/stampeded-v0.8.1...stampeded-v0.9.0) (2026-11-08)


### Features

* **http:** serve_with mounts the embedder's pages behind the dashboard token ([7b0ea01](https://github.com/seuros/stampeded/commit/7b0ea01e1ecd99582193fb24afe7211c0c48e89a))

## [0.8.1](https://github.com/seuros/stampeded/compare/stampeded-v0.8.0...stampeded-v0.8.1) (2026-11-06)


### Bug Fixes

* **deps:** move to chrono 0.11, breaker 0.24, deadline 0.4, mcp-host 0.23 ([a17199e](https://github.com/seuros/stampeded/commit/a17199ee550100bf083e8cfa075d517ac09f93a6))

## [0.8.0](https://github.com/seuros/stampeded/compare/stampeded-v0.7.2...stampeded-v0.8.0) (2026-11-06)


### Features

* **claim:** round robin across queues via priority-machines ([730e1fb](https://github.com/seuros/stampeded/commit/730e1fb51cf42927b3146f04ece0f15d5faa73ef))


### Bug Fixes

* **deps:** move to breaker 0.22, throttle 0.5, chrono 0.10, mcp-host 0.20 ([da1156d](https://github.com/seuros/stampeded/commit/da1156d35dc933f0b2fc31911a0e33923e3a2aea))

## [0.7.2](https://github.com/seuros/stampeded/compare/stampeded-v0.7.1...stampeded-v0.7.2) (2026-11-06)


### Bug Fixes

* serialise stampeded_create_queue so concurrent first opens cannot collide ([5aee1c5](https://github.com/seuros/stampeded/commit/5aee1c5ae4ce2e4d68c103db45d2edeadde11070))

## [0.7.1](https://github.com/seuros/stampeded/compare/stampeded-v0.7.0...stampeded-v0.7.1) (2026-11-05)


### Bug Fixes

* **game:** render Factory Kit models with their texture and recover dropped streams ([e8c3e0d](https://github.com/seuros/stampeded/commit/e8c3e0d39d8433e6d19223fa4539e2d3fdfa31f3))

## [0.7.0](https://github.com/seuros/stampeded/compare/stampeded-v0.6.0...stampeded-v0.7.0) (2026-11-03)


### Features

* propagate worker cancellation scopes ([ca73bb8](https://github.com/seuros/stampeded/commit/ca73bb817d7a357af6715ca4031e131cef296541))


### Bug Fixes

* preserve configured retry and expiry limits in scheduled jobs ([c50747d](https://github.com/seuros/stampeded/commit/c50747da9b616bde0ee21d6d31e027f6311e3121))
* retain recoverable claims and join executions during shutdown ([33288b5](https://github.com/seuros/stampeded/commit/33288b5bb52c9cd9ba2bf0ec8cdce1ba93884d05))

## [0.6.0](https://github.com/seuros/stampeded/compare/stampeded-v0.5.0...stampeded-v0.6.0) (2026-10-31)


### Features

* typed jobs with AsyncJob ([73e26a4](https://github.com/seuros/stampeded/commit/73e26a4d56a43f7973923c0b3ecc87d592e99373))

## [0.5.0](https://github.com/seuros/stampeded/compare/stampeded-v0.4.1...stampeded-v0.5.0) (2026-10-30)


### ⚠ BREAKING CHANGES

* public API changes since 0.4.1. ApiState::dashboard_guard is gone (the DashboardToken layer guards the dashboard); game::GameParams is gone and dashboard::index, game::index and game::stream lost their token parameters. JobLifecycleEvent has a Resurrect variant, EVENTS has eight entries, and stampeded_lifecycle_events() lists "resurrect". QueueSnapshot has a `paused` field. The daemon's planner reads schema 7: run it with --migrate.

### Features

* pause a queue ([e0d13c7](https://github.com/seuros/stampeded/commit/e0d13c72a5d0cb3a188f5d4328cef7941bfb147e))


### Bug Fixes

* a resurrected job gets every budget back ([d80729b](https://github.com/seuros/stampeded/commit/d80729bd71e65dd0ac60a33c5494349f1068e2e5))
* an unknown schedule timezone no longer stops every schedule ([3381671](https://github.com/seuros/stampeded/commit/3381671656d36afa2beeb60feca317f9fdb84b4d))
* build without libclang by taking only the rama features in use ([78dff18](https://github.com/seuros/stampeded/commit/78dff1818371737e8e6b0e4063d60761b322c5aa))
* **ruby:** log worker failures when stdout is not a terminal ([43322f7](https://github.com/seuros/stampeded/commit/43322f7b54f20c2ef3ec9cfeeb1d495141248eb5))
* **ruby:** recurring keyword arguments arrive as keywords ([eacf29c](https://github.com/seuros/stampeded/commit/eacf29c515f706f437822cfa51d2adf47d5a5bdd))
* **ruby:** resolve a schedule's queue_as with its arguments ([a571f50](https://github.com/seuros/stampeded/commit/a571f50f0b4195767e2dbe5f4edce68492c6d9a9))

## [0.4.1](https://github.com/seuros/stampeded/compare/stampeded-v0.4.0...stampeded-v0.4.1) (2026-10-29)


### Bug Fixes

* key dedup scopes by their text, not their bytea escapes ([#10](https://github.com/seuros/stampeded/issues/10)) ([87e3ccf](https://github.com/seuros/stampeded/commit/87e3ccfa5e472bafa07b309789c4ccc610d4e42f))

## [0.4.0](https://github.com/seuros/stampeded/compare/stampeded-v0.3.2...stampeded-v0.4.0) (2026-10-24)


### ⚠ BREAKING CHANGES

* with default features off, the library brings no TLS; enable `tls-aws-lc`, `tls-ring` or a sqlx TLS feature for TLS connections.

### Features

* schema::migrate is safe on every start, and upgrades in place ([4075aea](https://github.com/seuros/stampeded/commit/4075aea9bbe7f078c166349e0e53acd21574bbd1))
* the library leaves the Postgres TLS backend to the app ([a6ba009](https://github.com/seuros/stampeded/commit/a6ba009e3c3d6f1eb899662f844c4b10e6325915))


### Bug Fixes

* close a dedup_key forgery and harden schema::migrate ([c4c9679](https://github.com/seuros/stampeded/commit/c4c9679f1d12d2c8f1a9d92595cc1441e1b68bad))
* refuse to build with both rustls crypto-provider features enabled ([4ddc328](https://github.com/seuros/stampeded/commit/4ddc328fc9851bacce1978c4097124ed162dcf77))
* saturated workers wake when a slot frees, not just on NOTIFY or poll floor ([0b99400](https://github.com/seuros/stampeded/commit/0b994007769cf1d1cc745de4f7f0e5f517365326))
* unique jobs of different periods no longer share a key when their windows start together ([14d2ca4](https://github.com/seuros/stampeded/commit/14d2ca446b867c0eb6b2d09eea9deb8f6bab4a46))

## [0.3.2](https://github.com/seuros/stampeded/compare/stampeded-v0.3.1...stampeded-v0.3.2) (2026-10-19)


### Features

* dashboard tenants and trends, workers report their hostname ([8174070](https://github.com/seuros/stampeded/commit/817407069a8218813a801f20b8cb0937e8cc56ac))


### Bug Fixes

* escape credentials in the daemon's database URL ([3d0c30d](https://github.com/seuros/stampeded/commit/3d0c30dafd946eb1759ad15a28fa89c50bcb93a3))
* exec the daemon by path, never through a shell ([f5ab041](https://github.com/seuros/stampeded/commit/f5ab04101ef9d30aa8e4fd3435b8188d3689b419))
* hand the daemon its database URL through the environment, not argv ([a765e87](https://github.com/seuros/stampeded/commit/a765e8704f2978ea2a9a37787f738c3cf8bc5bb0))
* read the dashboard token from STAMPEDED_DASHBOARD_TOKEN ([e961880](https://github.com/seuros/stampeded/commit/e96188021bf2a73e2d18b6b15da8e79fb97d9701))
* the daemon keeps the app's TLS settings ([c4b5b39](https://github.com/seuros/stampeded/commit/c4b5b39f02c731c2fb2a324f44d27b476c3af274))

## [0.3.1](https://github.com/seuros/stampeded/compare/stampeded-v0.3.0...stampeded-v0.3.1) (2026-10-19)


### Bug Fixes

* release builds keep Cargo.lock in step with the version bump ([0a05b98](https://github.com/seuros/stampeded/commit/0a05b98e59b1f39a2bebe163ff62bdc364716164))

## [0.3.0](https://github.com/seuros/stampeded/compare/stampeded-v0.2.2...stampeded-v0.3.0) (2026-10-19)


### Features

* extract stampeded for open source, history squashed ([752728f](https://github.com/seuros/stampeded/commit/752728fff45f367b51a8b881b5915c2550a5601a))

## Changelog
