# frozen_string_literal: true

require "minitest/autorun"
require "securerandom"
require "tmpdir"
require "active_job"
require "stampeded"
require "stampeded/active_job_ext"
require "stampeded/active_job_worker"
require "active_job/queue_adapters/stampeded_adapter"

require "stampeded/native"

ActiveJob::Base.include(Stampeded::ActiveJobExt)
Stampeded::Native.log_init(ENV.fetch("STAMPEDED_LOG", "error"))
ActiveJob::Base.logger = Logger.new(IO::NULL)

module StampededTest
  URL = ENV.fetch("STAMPEDED_TEST_DATABASE_URL", nil)

  def self.require_database!
    return if URL
    return unless ENV["CI"]

    raise "STAMPEDED_TEST_DATABASE_URL is not set and CI is. Refusing to skip " \
          "the database tests and report success: that would be a green build " \
          "that ran nothing. Check the Postgres service container."
  end

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
