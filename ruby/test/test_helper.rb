# frozen_string_literal: true

require "minitest/autorun"
require "securerandom"
require "tmpdir"
require "active_job"
require "stampeded"
require "stampeded/active_job_ext"
require "stampeded/active_job_worker"
require "active_job/queue_adapters/stampeded_adapter"

ActiveJob::Base.include(Stampeded::ActiveJobExt)
ActiveJob::Base.logger = Logger.new(IO::NULL)

module StampededTest
  URL = ENV.fetch("STAMPEDED_TEST_DATABASE_URL", nil)

  # Skipping is a courtesy to someone without a database. It must never be a
  # courtesy to CI.
  #
  # Without this, `rake test` with the variable unset runs 19 of 78 tests,
  # reports "0 failures", and EXITS 0 -- a green suite that verified almost
  # nothing. CI sets the variable today, so the hazard is not that it is
  # missing; it is that renaming the variable, or a Postgres service container
  # that fails to come up, converts the entire Ruby suite into a no-op without
  # turning anything red. A green build that tested nothing is worse than a red
  # one, because it is believed.
  def self.require_database!
    return if URL
    return unless ENV["CI"]

    raise "STAMPEDED_TEST_DATABASE_URL is not set and CI is. Refusing to skip " \
          "the database tests and report success: that would be a green build " \
          "that ran nothing. Check the Postgres service container."
  end

  # A private schema per test class, so these can run alongside the Rust
  # integration tests against the same database without fighting over tables.
  module Isolation
    def setup
      StampededTest.require_database!
      skip "STAMPEDED_TEST_DATABASE_URL not set" unless URL

      @schema = "rb_#{SecureRandom.hex(8)}"
      admin = PG.connect(URL)
      admin.exec("CREATE SCHEMA #{@schema}")
      admin.close

      Stampeded.config.database_url = "#{URL}?options=-csearch_path%3D#{@schema}"
      Stampeded::Connection.own { |c| Stampeded::Schema.install!(c) }
      Stampeded.create_queue("default")
      super
    end

    def teardown
      super
      return unless URL && @schema

      admin = PG.connect(URL)
      # CASCADE prints a NOTICE per dropped object; there are ~30 of them.
      admin.exec("SET client_min_messages TO WARNING")
      admin.exec("DROP SCHEMA #{@schema} CASCADE")
      admin.close
    end

    def conn(&) = Stampeded::Connection.own(&)

    def job_row(id)
      conn { |c| c.exec_params("SELECT * FROM stampeded_jobs WHERE id = $1", [id])[0] }
    end
  end
end
