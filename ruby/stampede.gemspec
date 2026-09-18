# frozen_string_literal: true

require_relative "lib/stampede/version"

Gem::Specification.new do |spec|
  spec.name    = "stampede"
  spec.version = Stampede::VERSION
  spec.authors = ["Abdelkader Boudih"]
  spec.email   = ["terminale@gmail.com"]

  spec.summary     = "A polyglot job processor built on Postgres 18+"
  spec.description = <<~TEXT
    Rails integration for Stampede: an ActiveJob adapter, a worker that executes
    jobs, and a launcher for the precompiled daemon.

    Enqueueing is a plain INSERT in your own transaction, so a rolled-back
    transaction takes its jobs with it. Claiming goes through SQL functions
    shared with the Rust worker, so there is no second implementation of the
    attempt accounting or the ownership rules.
  TEXT
  spec.homepage = "https://github.com/seuros/stampede"
  spec.license  = "BSD-3-Clause"

  spec.required_ruby_version = ">= 3.2.0"

  spec.files = Dir["lib/**/*.rb", "lib/**/*.rake", "lib/generators/**/*", "schema/*.sql", "README.md", "LICENSE"]
  spec.require_paths = ["lib"]

  spec.add_dependency "pg", ">= 1.4"
  # The lifecycle is declared, not compared against string literals. Mirrors
  # crates/stampede/src/lifecycle.rs so the two workers cannot disagree.
  spec.add_dependency "state_machines", ">= 0.100"

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"
end
