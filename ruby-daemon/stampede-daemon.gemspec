# frozen_string_literal: true

require_relative "lib/stampede/daemon/version"

Gem::Specification.new do |spec|
  spec.name    = "stampede-daemon"
  spec.version = Stampede::Daemon::VERSION
  spec.authors = ["Abdelkader Boudih"]
  spec.email   = ["terminale@gmail.com"]

  spec.summary  = "Precompiled Stampede daemon binary"
  spec.description = <<~TEXT
    The Stampede daemon: scheduler, reaper, pruner, expirer, health checks,
    HTTP API and dashboard. All of those operate on rows rather than on job
    bodies, which is why they can live in a Rust binary while your jobs stay in
    Ruby.

    This gem exists so you do not need a Rust toolchain. It is OPTIONAL --
    enqueueing and running jobs work without it.
  TEXT
  spec.homepage = "https://github.com/seuros/stampede"
  spec.license  = "BSD-3-Clause"

  spec.required_ruby_version = ">= 3.2.0"

  # Platform gems get exe/<platform>/stampede; the generic gem ships none and
  # tells you how to build one.
  # One binary per platform, and NO Ruby-ABI dimension: this is a standalone
  # executable, not a linked extension. (breaker_machines and chrono_machines
  # ship per platform AND per Ruby ABI because they are extensions; a daemon
  # does not care which Ruby is running.)
  if (platform = ENV.fetch("STAMPEDE_GEM_PLATFORM", nil))
    spec.platform = platform
    spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"] +
                 Dir["exe/#{platform}/*"]
  else
    spec.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]
  end
  spec.require_paths = ["lib"]

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"
end
