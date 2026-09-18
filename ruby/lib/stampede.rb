# frozen_string_literal: true

require_relative "stampede/version"
require_relative "stampede/errors"
require_relative "stampede/configuration"
require_relative "stampede/connection"
require_relative "stampede/job"
require_relative "stampede/client"
require_relative "stampede/schema"
require_relative "stampede/schedule"
require_relative "stampede/worker"
require_relative "stampede/daemon"

require_relative "stampede/railtie" if defined?(Rails::Railtie)

# Stampede — a polyglot job processor on Postgres 18+.
#
# This gem is deliberately NOT a client library in the Faktory sense (plans/05).
# There is no wire protocol here: enqueueing is an `INSERT`, and claiming goes
# through SQL functions that the Rust worker calls too, so there is exactly one
# implementation of the attempt accounting, the fence and the ownership rules.
# If this gem vanished you could do everything it does with
# `connection.execute` and a downloaded binary. What it sells is the ten lines
# of glue that make `perform_later` work, and not needing a Rust toolchain.
module Stampede
  class << self
    def config
      @config ||= Configuration.new
    end

    def configure
      yield config
    end

    # Enqueue a job.
    #
    # Runs on the CALLER'S connection, which is the whole point: inside an
    # ActiveRecord transaction this INSERT commits — or rolls back — atomically
    # with the business data that caused it. A Redis-backed queue cannot do
    # that, which is why Sidekiq has warned about enqueueing inside
    # transactions for a decade.
    def enqueue(queue:, task:, **opts)
      Client.enqueue(queue: queue, task: task, **opts)
    end

    # Provision a queue's own partition.
    #
    # Optional: an unprovisioned queue still works via the DEFAULT partition.
    # But a dedicated partition gets its own fillfactor and autovacuum
    # settings, which are NOT inherited from the parent — a queue left on the
    # defaults waits for 20% dead tuples before vacuuming.
    def create_queue(name)
      Connection.with { |c| c.exec_params("SELECT stampede_create_queue($1)", [name]) }
      name
    end

    # Queue depth by state.
    #
    # Reads the counter table, never `count(*)` over the jobs table — an
    # unbounded aggregate there holds the xmin horizon open and stops vacuum
    # reclaiming, which is the documented way a Postgres queue slowly dies.
    def depth(queue)
      Connection.with do |c|
        c.exec_params(
          "SELECT state, n FROM stampede_queue_counts WHERE queue = $1 AND n <> 0 ORDER BY state",
          [queue]
        ).each_with_object({}) { |r, h| h[r["state"]] = r["n"].to_i }
      end
    end
  end
end
