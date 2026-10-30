# Changelog

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
