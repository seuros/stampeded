# frozen_string_literal: true

require "minitest/autorun"
require "securerandom"
require "tmpdir"
require "active_job"
require "stampede"
require "stampede/active_job_ext"
require "stampede/active_job_worker"
require "active_job/queue_adapters/stampede_adapter"

ActiveJob::Base.include(Stampede::ActiveJobExt)
ActiveJob::Base.logger = Logger.new(IO::NULL)

module StampedeTest
  URL = ENV.fetch("STAMPEDE_TEST_DATABASE_URL", nil)

  # A private schema per test class, so these can run alongside the Rust
  # integration tests against the same database without fighting over tables.
  module Isolation
    def setup
      skip "STAMPEDE_TEST_DATABASE_URL not set" unless URL

      @schema = "rb_#{SecureRandom.hex(8)}"
      admin = PG.connect(URL)
      admin.exec("CREATE SCHEMA #{@schema}")
      admin.close

      Stampede.config.database_url = "#{URL}?options=-csearch_path%3D#{@schema}"
      Stampede::Connection.own { |c| Stampede::Schema.install!(c) }
      Stampede.create_queue("default")
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

    def conn(&) = Stampede::Connection.own(&)

    def job_row(id)
      conn { |c| c.exec_params("SELECT * FROM stampede_jobs WHERE id = $1", [id])[0] }
    end
  end
end
