# frozen_string_literal: true

require_relative "stampeded/version"
require_relative "stampeded/errors"
require_relative "stampeded/configuration"
require_relative "stampeded/connection"
require "forwardable"
require_relative "stampeded/lifecycle"
require_relative "stampeded/job"
require_relative "stampeded/job_context"
require_relative "stampeded/current"
require_relative "stampeded/steps"
require_relative "stampeded/client"
require_relative "stampeded/schema"
require_relative "stampeded/schedule"
require_relative "stampeded/worker"
require_relative "stampeded/daemon"

module Stampeded
  # Resolvable whenever it is referenced, rather than whenever ActiveJob
  # happens to have been loaded.
  #
  # The railtie requires this inside `on_load(:active_job)`, which only fires
  # when ActiveJob::Base is first touched. A worker script that goes straight
  # to `Stampeded::ActiveJobWorker.new` never touches it, so in a lazily-loaded
  # development app the constant did not exist -- while the identical code
  # worked in production, where eager loading had already pulled ActiveJob in.
  # Found running a worker inside a real Rails app.
  autoload :ActiveJobWorker, File.expand_path("stampeded/active_job_worker", __dir__)
end

require_relative "stampeded/railtie" if defined?(Rails::Railtie)

# Stampeded — a polyglot job processor on Postgres 18+.
#
# This gem is deliberately NOT a client library in the Faktory sense (plans/05).
# There is no wire protocol here: enqueueing is an `INSERT`, and claiming goes
# through SQL functions that the Rust worker calls too, so there is exactly one
# implementation of the attempt accounting, the fence and the ownership rules.
# If this gem vanished you could do everything it does with
# `connection.execute` and a downloaded binary. What it sells is the ten lines
# of glue that make `perform_later` work, and not needing a Rust toolchain.
module Stampeded
  # Is a value Postgres returned truthy?
  #
  # NOT `== "t"`. The same query returns a different Ruby type depending on
  # WHICH CONNECTION ran it: ActiveRecord installs type decoders, so a boolean
  # comes back as TrueClass, while a bare PG.connect returns the string "t".
  # The gem uses both -- the caller's connection for enqueue, its own for the
  # worker -- so any comparison against "t" is correct in one context and
  # silently false in the other.
  #
  # That is not hypothetical: `Schema.installed?` always answered "no" inside
  # Rails, so the installer tried to re-apply the schema over itself and died
  # on the first table that already existed.
  def self.truthy?(value)
    value == true || value == "t" || value == "true"
  end

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

    # A libpq URL for the daemon, which takes one on the command line.
    #
    # Built from whatever Connection resolved rather than from config, so the
    # daemon and the workers cannot end up on different databases.
    def database_url_for_daemon
      params = Connection.connection_params
      return params if params.is_a?(String)

      user = [params[:user], params[:password]].compact.join(":")
      host = [params[:host], params[:port]].compact.join(":")
      "postgres://#{user}#{"@" unless user.empty?}#{host}/#{params[:dbname]}"
    end

    # Provision a queue's own partition.
    #
    # Optional: an unprovisioned queue still works via the DEFAULT partition.
    # But a dedicated partition gets its own fillfactor and autovacuum
    # settings, which are NOT inherited from the parent — a queue left on the
    # defaults waits for 20% dead tuples before vacuuming.
    def create_queue(name)
      Connection.with { |c| c.exec_params("SELECT stampeded_create_queue($1)", [name]) }
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
          "SELECT state, n FROM stampeded_queue_counts WHERE queue = $1 AND n <> 0 ORDER BY state",
          [queue]
        ).each_with_object({}) { |r, h| h[r["state"]] = r["n"].to_i }
      end
    end
  end
end
