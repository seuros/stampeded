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

    def install!(conn = nil)
      if conn
        conn.exec(sql)
      else
        Connection.with { |c| c.exec(sql) }
      end
    end

    def installed?(conn = nil)
      check = ->(c) { c.exec("SELECT to_regclass('stampede_jobs') IS NOT NULL AS ok")[0]["ok"] == "t" }
      conn ? check.call(conn) : Connection.with(&check)
    end
  end
end
