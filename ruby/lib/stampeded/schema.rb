# frozen_string_literal: true

module Stampeded
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
    # Found by hitting exactly that: a database with stampeded_workers and no
    # stampeded_jobs, unable to install and unable to report why.
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

    # Drop everything Stampeded owns.
    #
    # For tests and for recovering a half-applied install. Deliberately not
    # wired to a rake task: it deletes every job.
    def drop!(conn = nil)
      run = lambda do |c|
        # Functions are dropped by PATTERN, not by a list.
        #
        # This was a hand-maintained list of twenty names, and adding a
        # twenty-first function broke it exactly as you would expect: install,
        # drop, install again, and the second install failed with
        # "function stampeded_partition_health already exists". The list has no
        # way to stay correct -- nothing links it to the schema files, so it
        # rots silently until a test that reinstalls happens to run.
        #
        # Scoped to current_schema() because the test suite installs into a
        # private schema per class, and a pattern that ignored that would drop
        # another test's functions mid-run.
        c.exec(<<~SQL)
          DROP TABLE IF EXISTS stampeded_cron_occurrences, stampeded_schedules,
                               stampeded_queue_counts, stampeded_jobs, stampeded_workers CASCADE;
          DO $$
          DECLARE r record;
          BEGIN
            FOR r IN
              SELECT p.oid::regprocedure AS sig
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
               WHERE p.proname LIKE 'stampeded\_%'
                 AND n.nspname = current_schema()
            LOOP
              EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig || ' CASCADE';
            END LOOP;
          END $$;
        SQL
      end
      conn ? run.call(conn) : Connection.with(&run)
    end

    def installed?(conn = nil)
      check = lambda do |c|
        Stampeded.truthy?(c.exec("SELECT to_regclass('stampeded_jobs') IS NOT NULL AS ok")[0]["ok"])
      end
      conn ? check.call(conn) : Connection.with(&check)
    end
  end
end
