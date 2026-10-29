# frozen_string_literal: true

require "digest"

module Stampeded
  module Schema
    # Where the daemon's sqlx migrator records what it applied.
    TABLE = "stampeded_migrations"

    module_function

    def root
      candidates = [
        File.expand_path("../../schema", __dir__),
        File.expand_path("../../../crates/stampeded/schema", __dir__)
      ]
      candidates.find { |d| File.directory?(d) } ||
        raise(Error, "cannot find schema/*.sql (looked in #{candidates.join(", ")})")
    end

    # [version, description, sql] per migration, in order: sqlx's names.
    def migrations
      Dir[File.join(root, "*.sql")].sort.map do |path|
        version, name = File.basename(path, ".sql").split("_", 2)
        [Integer(version, 10), name.tr("_", " "), File.read(path)]
      end
    end

    def sql = migrations.map(&:last).join("\n")

    # A fresh install, recorded as sqlx records it, so the daemon's
    # --migrate finds nothing to apply.
    def install!(conn = nil)
      run = lambda do |c|
        c.exec("BEGIN")
        begin
          c.exec(<<~SQL)
            CREATE TABLE #{TABLE} (
                version BIGINT PRIMARY KEY,
                description TEXT NOT NULL,
                installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
                success BOOLEAN NOT NULL,
                checksum BYTEA NOT NULL,
                execution_time BIGINT NOT NULL
            )
          SQL
          migrations.each do |version, description, sql|
            c.exec(sql)
            c.exec_params(
              "INSERT INTO #{TABLE} (version, description, success, checksum, execution_time) " \
              "VALUES ($1, $2, true, decode($3, 'hex'), 0)",
              [version, description, Digest::SHA384.hexdigest(sql)]
            )
          end
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
                               stampeded_jobs, stampeded_workers,
                               stampeded_schema, stampeded_migrations CASCADE;
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
