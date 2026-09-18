# frozen_string_literal: true

require "test_helper"

# The `stampeded` class-level DSL.
#
# These need no database: they are about what the DSL accepts, which is where
# the bug was.
class ActiveJobExtTest < Minitest::Test
  # The migration path out of retry_on must actually work.
  #
  # Stampeded::RetryConflict -- raised when a job declares retry_on -- tells the
  # user to write exactly this. `backoff_base_ms` and `backoff_max_ms` were
  # missing from the DSL's allow-list, so following those instructions raised
  # "unknown stampeded options: backoff_base_ms, backoff_max_ms". The documented
  # way off retry_on did not work, and the only way to find it was to type what
  # the error message says.
  def test_the_retry_on_migration_example_is_accepted_verbatim
    klass = Class.new(ActiveJob::Base) { def self.name = "ChargeCard" }

    klass.stampeded max_attempts: 5, backoff_base_ms: 1_000, backoff_max_ms: 300_000

    assert_equal 1_000, klass.stampeded_options[:backoff_base_ms]
    assert_equal 300_000, klass.stampeded_options[:backoff_max_ms]
  end

  # backoff_base_ms is a smallint on the row. Caught here, at class-load, by
  # the person who wrote it -- rather than as PG::NumericValueOutOfRange at
  # enqueue time, which is to say in production, naming neither the option nor
  # the limit.
  def test_an_out_of_range_backoff_base_is_refused_with_an_explanation
    klass = Class.new(ActiveJob::Base) { def self.name = "TooSlow" }

    err = assert_raises(ArgumentError) { klass.stampeded backoff_base_ms: 60_000 }

    assert_match(/32767/, err.message)
    assert_match(/backoff_max_ms/, err.message, "should point at the option they meant")
  end

  def test_unknown_options_are_still_refused
    klass = Class.new(ActiveJob::Base) { def self.name = "Typo" }

    err = assert_raises(ArgumentError) { klass.stampeded max_attemtps: 5 }

    assert_match(/max_attemtps/, err.message)
  end
end
