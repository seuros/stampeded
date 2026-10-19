# frozen_string_literal: true

require "test_helper"

class ActiveJobExtTest < Minitest::Test
  def test_the_retry_on_migration_example_is_accepted_verbatim
    klass = Class.new(ActiveJob::Base) { def self.name = "ChargeCard" }

    klass.stampeded max_attempts: 5, backoff_base_ms: 1_000, backoff_max_ms: 300_000

    assert_equal 1_000, klass.stampeded_options[:backoff_base_ms]
    assert_equal 300_000, klass.stampeded_options[:backoff_max_ms]
  end

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
