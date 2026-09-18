# frozen_string_literal: true

module Stampede
  # Applies the SQL schema.
  #
  # The gem ships schema/*.sql VERBATIM and applies it, rather than carrying a
  # Rails migration that recreates the tables. A gem-maintained migration would
  # be a SECOND definition of the schema, on its own release cycle, free to
  # drift from the one the Rust daemon is built against (plans/16 §5).
  module Schema
    FILES = %w[001_initial.sql 002_operations.sql 003_schedules.sql].freeze

    module_function

    def root
      # Installed gem layout first, then the repo layout for development.
      candidates = [
        File.expand_path("../../schema", __dir__),
        File.expand_path("../../../schema", __dir__)
      ]
      candidates.find { |d| File.directory?(d) } ||
        raise(Error, "cannot find schema/*.sql (looked in #{candidates.join(", ")})")
    end

    def sql
      FILES.map { |f| File.read(File.join(root, f)) }.join("\n")
    end

    # Apply the schema, ALL OR NOTHING.
    #
    # Postgres has transactional DDL, and this needs it. Without the
    # transaction a failure partway leaves some tables created and some not --
    # and since the schema uses bare CREATE TABLE, you then cannot re-run it:
    # every attempt dies on the first object that already exists, and
    # `installed?` says no because the table it checks was never reached.
    # The only way out is dropping things by hand.
    #
    # Found by hitting exactly that: a database with stampede_workers and no
    # stampede_jobs, unable to install and unable to report why.
    def install!(conn = nil)
      run = lambda do |c|
        c.exec("BEGIN")
        begin
          c.exec(sql)
          c.exec("COMMIT")
        rescue StandardError
          c.exec("ROLLBACK")
          raise
        end
      end

      conn ? run.call(conn) : Connection.with(&run)
    end

    # Drop everything Stampede owns.
    #
    # For tests and for recovering a half-applied install. Deliberately not
    # wired to a rake task: it deletes every job.
    def drop!(conn = nil)
      run = lambda do |c|
        c.exec(<<~SQL)
          DROP TABLE IF EXISTS stampede_cron_occurrences, stampede_schedules,
                               stampede_queue_counts, stampede_jobs, stampede_workers CASCADE;
          DROP FUNCTION IF EXISTS stampede_claim, stampede_complete, stampede_fail,
                                  stampede_touch, stampede_expire, stampede_reap,
                                  stampede_prune, stampede_heartbeat, stampede_checkpoint,
                                  stampede_fingerprint, stampede_vacuum_health,
                                  stampede_index_density, stampede_create_queue,
                                  stampede_notify_insert, stampede_count_delta,
                                  stampede_attach_counters, stampede_recount_queue,
                                  stampede_claim_due_schedules, stampede_fire_schedule,
                                  stampede_prune_occurrences CASCADE;
        SQL
      end
      conn ? run.call(conn) : Connection.with(&run)
    end

    def installed?(conn = nil)
      check = lambda do |c|
        Stampede.truthy?(c.exec("SELECT to_regclass('stampede_jobs') IS NOT NULL AS ok")[0]["ok"])
      end
      conn ? check.call(conn) : Connection.with(&check)
    end
  end
end
