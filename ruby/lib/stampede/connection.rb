# frozen_string_literal: true

require "pg"

module Stampede
  # Where SQL goes.
  #
  # Two paths, and the distinction is load-bearing rather than a convenience:
  #
  #   * ENQUEUE uses ActiveRecord's current connection when Rails is loaded, so
  #     the INSERT lands inside whatever transaction the caller already has
  #     open. That is the property a Redis-backed queue cannot offer, and
  #     taking our own connection here would silently throw it away -- the job
  #     would commit even when the transaction that created it rolled back.
  #
  #   * The WORKER owns its own connection. It is a separate process with no
  #     caller transaction to join, and it must not hold an ActiveRecord
  #     connection out of the pool for the lifetime of a long job (underway's
  #     documented trap: a pinned connection per in-flight job exhausts
  #     max_connections with a handful of processes).
  module Connection
    module_function

    # Run against the caller's connection where there is one.
    def with(&block)
      if active_record?
        ActiveRecord::Base.connection.raw_connection.then(&block)
      else
        own(&block)
      end
    end

    # Always a dedicated connection. For the worker.
    def own
      conn = PG.connect(Stampede.config.database_url)
      begin
        yield conn
      ensure
        conn.close
      end
    end

    def active_record?
      defined?(ActiveRecord::Base) &&
        ActiveRecord::Base.respond_to?(:connection) &&
        ActiveRecord::Base.connected?
    rescue StandardError
      false
    end
  end
end
