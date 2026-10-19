# frozen_string_literal: true

require "test_helper"

class SchemaTest < Minitest::Test
  include StampededTest::Isolation

  def test_a_failed_install_leaves_nothing_behind
    conn { |c| Stampeded::Schema.drop!(c) }
    refute Stampeded::Schema.installed?

    broken = "#{Stampeded::Schema.sql}\nCREATE TABLE stampeded_jobs (oops int);"
    conn do |c|
      assert_raises(PG::Error) do
        c.exec("BEGIN")
        begin
          c.exec(broken)
          c.exec("COMMIT")
        rescue StandardError
          c.exec("ROLLBACK")
          raise
        end
      end
    end

    refute Stampeded::Schema.installed?
    left = conn do |c|
      c.exec(
        "SELECT count(*) FROM information_schema.tables " \
        "WHERE table_name LIKE 'stampeded%' AND table_schema = current_schema()"
      )[0]["count"].to_i
    end
    assert_equal 0, left, "a rolled-back install must leave no tables"

    conn { |c| Stampeded::Schema.install!(c) }
    assert Stampeded::Schema.installed?
  end

  def test_installed_is_honest_about_a_missing_schema
    conn { |c| Stampeded::Schema.drop!(c) }
    refute Stampeded::Schema.installed?
    conn { |c| Stampeded::Schema.install!(c) }
    assert Stampeded::Schema.installed?
  end
end

class SchemaMissingTest < Minitest::Test
  include StampededTest::Isolation

  def test_enqueueing_without_a_schema_says_what_to_run
    conn { |c| Stampeded::Schema.drop!(c) }

    error = assert_raises(Stampeded::SchemaMissing) do
      Stampeded.enqueue(queue: "default", task: "x")
    end

    assert_match(/stampeded:install/, error.message, "must name the Rails task")
    assert_match(/Schema\.install!/, error.message, "and the non-Rails call")
    assert_match(/Postgres 18/, error.message,
                 "the other likely cause is an old server, where it cannot apply at all")
  end

  def test_a_worker_fails_at_startup_rather_than_on_the_first_claim
    conn { |c| Stampeded::Schema.drop!(c) }

    assert_raises(Stampeded::SchemaMissing) do
      Stampeded::Worker.new(queue: "default", poll_interval: 0.05).run
    end
  end

  def test_unrelated_database_errors_are_not_swallowed
    error = assert_raises(PG::UndefinedTable) do
      Stampeded::Connection.translating_errors do
        Stampeded::Connection.with { |c| c.exec("SELECT * FROM some_other_app_table") }
      end
    end
    assert_match(/some_other_app_table/, error.message)
  end

  def test_drop_removes_every_stampeded_function_not_a_hardcoded_list
    Stampeded::Connection.own do |c|
      before = stampeded_functions(c)

      assert_operator before, :>, 15, "expected the schema to define functions"

      Stampeded::Schema.drop!(c)

      assert_equal 0, stampeded_functions(c),
                   "drop! left functions behind; it is dropping a list rather than a pattern"

      Stampeded::Schema.install!(c)

      assert_equal before, stampeded_functions(c), "reinstall did not restore the schema"
    end
  end

  def stampeded_functions(conn)
    conn.exec(<<~SQL)[0]["count"].to_i
      SELECT count(*) FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE p.proname LIKE 'stampeded\\_%' AND n.nspname = current_schema()
    SQL
  end
end
