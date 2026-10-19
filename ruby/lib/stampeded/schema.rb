# frozen_string_literal: true

module Stampeded
  module Schema
    FILES = %w[schema.sql].freeze

    module_function

    def root
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

    def drop!(conn = nil)
      run = lambda do |c|
        c.exec(<<~SQL)
          DROP TABLE IF EXISTS stampeded_cron_occurrences, stampeded_schedules,
                               stampeded_queue_policies, stampeded_queue_counts,
                               stampeded_jobs, stampeded_workers CASCADE;
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
