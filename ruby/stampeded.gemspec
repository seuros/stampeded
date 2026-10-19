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

  spec.required_ruby_version = ">= 4.0"
  spec.files = Dir["lib/**/*.rb", "lib/**/*.rake", "lib/generators/**/*",
                   "schema/*.sql", "README.md", "LICENSE"]

  spec.bindir = "exe"
  spec.executables = ["stampeded"]

  if (gem_platform = ENV.fetch("STAMPEDED_GEM_PLATFORM", nil))
    spec.platform = gem_platform
    spec.files += ["libexec/stampeded",
                   gem_platform.include?("darwin") ? "libexec/libstampeded_ffi.dylib" : "libexec/libstampeded_ffi.so"]
  end
  spec.require_paths = ["lib"]

  spec.add_dependency "pg", ">= 1.4"
  spec.add_dependency "ffi", ">= 1.17"

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["rubygems_mfa_required"] = "true"
end
