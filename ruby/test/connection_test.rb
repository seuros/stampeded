# frozen_string_literal: true

require "test_helper"

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
  include StampededTest::Isolation

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
    Stampeded.config.database_url = "postgres://someone@elsewhere/wrong_db"

    with_active_record do
      params = Stampeded::Connection.connection_params

      assert_kind_of Hash, params
      assert_equal "app_db", params[:dbname], "the app's database must win"
      assert_equal "apphost", params[:host]
      refute_match(/wrong_db/, params.to_s)
    end
  end

  def test_an_explicit_url_is_honoured_outside_rails
    Stampeded.config.database_url = "postgres://u:p@h/db"
    assert_equal "postgres://u:p@h/db", Stampeded::Connection.connection_params
  end

  def test_no_configuration_at_all_says_what_to_do
    Stampeded.config.database_url = nil
    error = assert_raises(Stampeded::Error) { Stampeded::Connection.connection_params }
    assert_match(/STAMPEDED_DATABASE_URL/, error.message)
  end

  def test_the_daemon_url_is_built_from_the_same_source_as_the_workers
    with_active_record(host: "db.internal", database: "prod", username: "app") do
      url = Stampeded.database_url_for_daemon
      assert_includes url, "db.internal"
      assert_includes url, "/prod"
      assert_includes url, "app:"
    end
  end
end

class TruthyTest < Minitest::Test
  def test_booleans_are_recognised_from_either_connection_type
    assert Stampeded.truthy?(true), "ActiveRecord's decoded boolean"
    assert Stampeded.truthy?("t"), "a bare PG connection's string"
    assert Stampeded.truthy?("true")

    refute Stampeded.truthy?(false)
    refute Stampeded.truthy?("f")
    refute Stampeded.truthy?(nil), "a function returning NULL is not success"
  end
end

class NoRailsTest < Minitest::Test
  def test_the_gem_declares_only_what_it_needs
    spec = Gem::Specification.load(File.expand_path("../stampeded.gemspec", __dir__))
    declared = spec.dependencies.select { |d| d.type == :runtime }.map(&:name).sort

    assert_equal %w[ffi pg], declared

    refute_includes declared, "activerecord",
                    "ActiveRecord must stay optional: Connection uses it only when defined"
    refute_includes declared, "rails"
    refute_includes declared, "activejob",
                    "the adapter file requires active_job; the gem itself must not"
  end

end

class ConnectionClassTest < Minitest::Test
  def teardown
    Stampeded.config.connection_class = nil
    Object.send(:remove_const, :StampededTestRecord) if Object.const_defined?(:StampededTestRecord)
  end

  def test_connection_class_names_the_class_whose_connection_is_used
    Object.const_set(:StampededTestRecord, Class.new)

    Stampeded.config.connection_class = "StampededTestRecord"

    assert_equal StampededTestRecord, Stampeded::Connection.ar_class
  end

  def test_the_default_is_the_primary_database
    skip "ActiveRecord not loaded in this file" unless defined?(::ActiveRecord::Base)

    assert_equal ::ActiveRecord::Base, Stampeded::Connection.ar_class
  end

  def test_a_missing_connection_class_is_named_in_the_error
    Stampeded.config.connection_class = "NoSuchRecordClass"

    err = assert_raises(Stampeded::Error) { Stampeded::Connection.ar_class }

    assert_match(/NoSuchRecordClass/, err.message)
  end
end
