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
