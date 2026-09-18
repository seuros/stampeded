# frozen_string_literal: true

require "test_helper"

# A stand-in for ActiveRecord, so these run without pulling Rails in.
module FakeAR
  class Base
    class << self
      attr_accessor :db_config, :connected, :raw

      def connection_db_config = db_config
      def connection = raw
    end
  end
end

class ConnectionTest < Minitest::Test
  include StampedeTest::Isolation

  def teardown
    Object.send(:remove_const, :ActiveRecord) if Object.const_defined?(:ActiveRecord)
    super
  end

  def with_active_record(host: "apphost", database: "app_db", username: "appuser")
    cfg = Struct.new(:configuration_hash).new(
      { host: host, database: database, username: username, password: "pw" }
    )
    FakeAR::Base.db_config = cfg
    Object.const_set(:ActiveRecord, FakeAR)
    yield
  end

  def test_active_record_wins_over_database_url
    # THE bug this exists for. An earlier version preferred
    # Stampede.config.database_url, so in a Rails app with DATABASE_URL set in
    # the shell -- which is extremely common -- jobs were written to a
    # DIFFERENT database than the app was using. The enqueue succeeded, the
    # schema installed "successfully" in the wrong place, and the enqueue
    # silently stopped being transactional. Nothing errored.
    Stampede.config.database_url = "postgres://someone@elsewhere/wrong_db"

    with_active_record do
      params = Stampede::Connection.connection_params

      assert_kind_of Hash, params
      assert_equal "app_db", params[:dbname], "the app's database must win"
      assert_equal "apphost", params[:host]
      refute_match(/wrong_db/, params.to_s)
    end
  end

  def test_an_explicit_url_is_honoured_outside_rails
    Stampede.config.database_url = "postgres://u:p@h/db"
    assert_equal "postgres://u:p@h/db", Stampede::Connection.connection_params
  end

  def test_no_configuration_at_all_says_what_to_do
    Stampede.config.database_url = nil
    error = assert_raises(Stampede::Error) { Stampede::Connection.connection_params }
    assert_match(/STAMPEDE_DATABASE_URL/, error.message)
  end

  def test_the_daemon_url_is_built_from_the_same_source_as_the_workers
    # The daemon takes a URL on the command line. If it were built from config
    # while the workers resolved through ActiveRecord, the scheduler and the
    # workers would run against different databases -- the scheduler would fire
    # jobs nobody ever claims.
    with_active_record(host: "db.internal", database: "prod", username: "app") do
      url = Stampede.database_url_for_daemon
      assert_includes url, "db.internal"
      assert_includes url, "/prod"
      assert_includes url, "app:"
    end
  end
end
