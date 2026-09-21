# frozen_string_literal: true

require_relative "lib/stampeded/version"

Gem::Specification.new do |spec|
  spec.name    = "stampeded"
  spec.version = Stampeded::VERSION
  spec.authors = ["Abdelkader Boudih"]
  spec.email   = ["terminale@gmail.com"]

  spec.summary     = "A polyglot job processor built on Postgres 18+"
  spec.description = <<~TEXT
    Rails integration for Stampeded: an ActiveJob adapter, a worker that executes
    jobs, and a launcher for the precompiled daemon.

    Enqueueing is a plain INSERT in your own transaction, so a rolled-back
    transaction takes its jobs with it. Claiming goes through SQL functions
    shared with the Rust worker, so there is no second implementation of the
    attempt accounting or the ownership rules.
  TEXT
  spec.homepage = "https://github.com/seuros/stampeded"
  spec.license  = "BSD-3-Clause"

  # Ruby 4.0+ only. Deliberately narrow rather than "whatever still parses":
  # supporting a version means testing it, and the CI matrix is the honest
  # statement of what is supported.
  spec.required_ruby_version = ">= 4.0"
  # ONE gem, with the binary in it.
  #
  # This was two gems -- `stampeded` pure Ruby and `stampeded-daemon` shipping
  # the binary per platform, following the tailwindcss-rails/tailwindcss-ruby
  # split. The argument was that a deployment already running the daemon
  # elsewhere should not download a binary per app server just to get
  # `perform_later`. Not worth a second package to publish, version and keep in
  # step for a few MB: people who need the daemon have it, people who do not
  # never run it.
  #
  # One binary per platform, and NO Ruby-ABI dimension: this is a standalone
  # executable, not a linked extension. (breaker_machines and chrono_machines
  # ship per platform AND per Ruby ABI because they are extensions; a daemon
  # does not care which Ruby is running.)
  #
  # `schema/` and `exe/` are STAGED into this directory by `rake stage` before
  # `gem build` -- they are not in git. The schema lives at the repo root
  # because the Rust side applies the same files; copying rather than
  # symlinking keeps `gem build` working from a tarball.
  #
  # The schema staging is load-bearing and was missing: `Dir["schema/*.sql"]`
  # matched nothing, so the published gem shipped ZERO .sql files while
  # Schema.install! looked for them at <gem_root>/schema. It worked in
  # development only because that lookup falls back to the repo checkout, so
  # every test passed and every gem user would have hit
  # "cannot find schema/*.sql" on install.
  spec.files = Dir["lib/**/*.rb", "lib/**/*.rake", "lib/generators/**/*",
                   "schema/*.sql", "README.md", "LICENSE"]

  # The launcher is a real executable, so `bundle exec stampeded` works and the
  # daemon can be run without Rails. Shipping the binary WITHOUT declaring an
  # executable left the gem with no command at all -- verified by installing it
  # into an empty GEM_HOME and finding bin/ empty -- so the only way to start
  # the daemon was a Rails rake task.
  spec.bindir = "exe"
  spec.executables = ["stampeded"]

  if (gem_platform = ENV.fetch("STAMPEDED_GEM_PLATFORM", nil))
    spec.platform = gem_platform
    # One binary, no platform subdirectory: a platform gem only ever contains
    # its own. The subdirectory was vestigial from tailwindcss-ruby.
    spec.files += ["libexec/stampeded"]
  end
  spec.require_paths = ["lib"]

  # ActiveJob is optional at runtime -- the plain-Ruby worker needs none of it
  # -- so it is not a hard dependency. But where the adapter DOES touch
  # ActiveJob it targets 8.1+, which is what the railtie and the adapter are
  # written and tested against.
  spec.add_dependency "pg", ">= 1.4"
  # The lifecycle is declared, not compared against string literals. Mirrors
  # crates/stampeded/src/lifecycle.rs so the two workers cannot disagree.
  spec.add_dependency "state_machines", ">= 0.100"

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"
end
