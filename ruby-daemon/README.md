# stampede-daemon

The precompiled [Stampede](https://github.com/seuros/stampede) daemon.

```ruby
gem "stampede-daemon"
```

One binary per platform, so you do not need a Rust toolchain. **Optional** —
enqueueing and running jobs work without it; you lose cron, orphan recovery,
retention and the dashboard.

It runs the roles that operate on *rows* rather than on job bodies, and is
therefore language-agnostic: scheduler, reaper, pruner, expirer, health checks,
HTTP API, dashboard. Your jobs stay in Ruby, run by `bin/rails stampede:work` —
see the `stampede` gem.

## Platforms

`arm64-darwin`, `x86_64-darwin`, and `{aarch64,x86_64}-linux-{gnu,musl}`.

musl is not an afterthought: most container images are Alpine, and a gnu binary
there fails at `exec` with a linker error rather than anything actionable.

For anything else, build from source and point at it:

```bash
cargo build --release
export STAMPEDE_INSTALL_DIR=/path/to/stampede/target/release
```

## Building the gems

```bash
rake platforms            # what we ship
rake 'build[arm64-darwin]'
rake 'package[arm64-darwin]'
```

Note there is no Ruby-ABI dimension here: this is a standalone executable, not
a linked extension, so it is one gem per platform rather than one per platform
× Ruby version.
