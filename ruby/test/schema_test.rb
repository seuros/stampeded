# frozen_string_literal: true

require "test_helper"

class SchemaTest < Minitest::Test
  include StampedeTest::Isolation

  def test_a_failed_install_leaves_nothing_behind
    # Without a transaction, a failure partway leaves some tables created and
    # some not -- and since the schema uses bare CREATE TABLE you then cannot
    # re-run it: every attempt dies on the first object that already exists,
    # and `installed?` says no because the table it checks was never reached.
    # The only way out is dropping things by hand.
    #
    # Found by hitting exactly that state in a real Rails app.
    conn { |c| Stampede::Schema.drop!(c) }
    refute Stampede::Schema.installed?

    broken = "#{Stampede::Schema.sql}\nCREATE TABLE stampede_jobs (oops int);"
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

    # Nothing survived, so a retry is possible.
    refute Stampede::Schema.installed?
    left = conn do |c|
      # SCOPED. information_schema.tables spans every schema in the database,
      # and this suite leaves one per test class behind while it runs -- the
      # same trap stampede_vacuum_health had.
      c.exec(
        "SELECT count(*) FROM information_schema.tables " \
        "WHERE table_name LIKE 'stampede%' AND table_schema = current_schema()"
      )[0]["count"].to_i
    end
    assert_equal 0, left, "a rolled-back install must leave no tables"

    # And the real one still works afterwards.
    conn { |c| Stampede::Schema.install!(c) }
    assert Stampede::Schema.installed?
  end

  def test_installed_is_honest_about_a_missing_schema
    conn { |c| Stampede::Schema.drop!(c) }
    refute Stampede::Schema.installed?
    conn { |c| Stampede::Schema.install!(c) }
    assert Stampede::Schema.installed?
  end
end

class SchemaMissingTest < Minitest::Test
  include StampedeTest::Isolation

  def test_enqueueing_without_a_schema_says_what_to_run
    # The single most likely failure on a first run. The raw Postgres error --
    # `relation "stampede_jobs" does not exist` -- is accurate and tells a
    # first-time user nothing.
    conn { |c| Stampede::Schema.drop!(c) }

    error = assert_raises(Stampede::SchemaMissing) do
      Stampede.enqueue(queue: "default", task: "x")
    end

    assert_match(/stampede:install/, error.message, "must name the Rails task")
    assert_match(/Schema\.install!/, error.message, "and the non-Rails call")
    assert_match(/Postgres 18/, error.message,
                 "the other likely cause is an old server, where it cannot apply at all")
  end

  def test_a_worker_fails_at_startup_rather_than_on_the_first_claim
    # A worker that cannot register will never do anything, so it should say so
    # immediately instead of producing a backtrace per poll forever.
    conn { |c| Stampede::Schema.drop!(c) }

    assert_raises(Stampede::SchemaMissing) do
      Stampede::Worker.new(queue: "default", poll_interval: 0.05).run
    end
  end

  def test_unrelated_database_errors_are_not_swallowed
    # Making one error friendlier must not hide the others. A missing table
    # that is not ours is somebody else's problem and should surface as itself.
    error = assert_raises(PG::UndefinedTable) do
      Stampede::Connection.translating_errors do
        Stampede::Connection.with { |c| c.exec("SELECT * FROM some_other_app_table") }
      end
    end
    assert_match(/some_other_app_table/, error.message)
  end

  # drop! must remove EVERY stampede function, not a list someone maintains.
  #
  # It was a hardcoded list of twenty names, and adding a twenty-first broke
  # install -> drop -> install with "function already exists". Nothing linked
  # the list to the schema files, so it rotted silently until a test that
  # reinstalls happened to run. This asserts the property instead of the list.
  def test_drop_removes_every_stampede_function_not_a_hardcoded_list
    Stampede::Connection.own do |c|
      before = stampede_functions(c)

      assert_operator before, :>, 15, "expected the schema to define functions"

      Stampede::Schema.drop!(c)

      assert_equal 0, stampede_functions(c),
                   "drop! left functions behind; it is dropping a list rather than a pattern"

      # The real symptom: a second install must succeed.
      Stampede::Schema.install!(c)

      assert_equal before, stampede_functions(c), "reinstall did not restore the schema"
    end
  end

  def stampede_functions(conn)
    conn.exec(<<~SQL)[0]["count"].to_i
      SELECT count(*) FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE p.proname LIKE 'stampede\\_%' AND n.nspname = current_schema()
    SQL
  end
end
