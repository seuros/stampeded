# frozen_string_literal: true

require "pg"

module Stampede
  # Where SQL goes.
  #
  # Two paths, and the distinction is load-bearing rather than a convenience:
  #
  #   * ENQUEUE uses ActiveRecord's current connection whenever ActiveRecord is
  #     present, so the INSERT lands inside whatever transaction the caller
  #     already has open. That is the property a Redis-backed queue cannot
  #     offer, and taking our own connection here would silently throw it away
  #     -- the job would commit even when the transaction that created it
  #     rolled back.
  #
  #   * The WORKER owns its own connection. It is a separate process with no
  #     caller transaction to join, and it must not hold an ActiveRecord
  #     connection out of the pool for the lifetime of a long job (underway's
  #     documented trap: a pinned pooled connection per in-flight job exhausts
  #     max_connections with a handful of processes).
  module Connection
    module_function

    # Run against the caller's connection where there is one.
    #
    # NOTE the absence of a `connected?` check. An earlier version had one, and
    # it was a serious bug: Rails connects LAZILY, so on the first call in a
    # fresh process `connected?` is false and this fell through to a private
    # connection built from ENV["DATABASE_URL"]. In a Rails app that variable
    # frequently points somewhere else -- or nowhere -- so jobs were written to
    # a different database than the one the app was using, the enqueue silently
    # stopped being transactional, and `stampede:install` reported success
    # having created the schema in the wrong place. Nothing errored.
    #
    # `ActiveRecord::Base.connection` establishes the connection on demand,
    # which is exactly what we want and what the check was preventing.
    def with(&block)
      if active_record?
        ActiveRecord::Base.connection.raw_connection.then(&block)
      else
        own(&block)
      end
    end

    # Always a dedicated connection. For the worker.
    #
    # Prefers ActiveRecord's configuration over `database_url` for the same
    # reason: in a Rails app the app's own database.yml is authoritative, and
    # it usually has no `url` key at all -- host/database/username instead --
    # so deriving the URL from ENV would connect somewhere else.
    def own
      conn = PG.connect(connection_params)
      begin
        yield conn
      ensure
        conn.close
      end
    end

    # What a private connection should dial.
    #
    # ActiveRecord wins when it is present, even if database_url is also set:
    # in a Rails app the app's own configuration is the answer to "which
    # database", and an env var that disagrees is a misconfiguration rather
    # than an override. An explicit url is honoured outside Rails.
    def connection_params
      explicit = Stampede.config.database_url

      if active_record?
        cfg = ActiveRecord::Base.connection_db_config.configuration_hash
        # PG wants its own key names; ActiveRecord's differ.
        return {
          host: cfg[:host] || "localhost",
          port: cfg[:port],
          dbname: cfg[:database],
          user: cfg[:username],
          password: cfg[:password]
        }.compact
      end

      explicit or raise Error, <<~MSG
        No database configured.

        Set Stampede.config.database_url, or STAMPEDE_DATABASE_URL, or run
        inside a Rails app where ActiveRecord is configured.
      MSG
    end

    def active_record?
      defined?(ActiveRecord::Base) && ActiveRecord::Base.respond_to?(:connection_db_config)
    rescue StandardError
      false
    end
  end
end
