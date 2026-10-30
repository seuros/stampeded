# frozen_string_literal: true

require "test_helper"

class LifecycleTest < Minitest::Test
  def test_a_retryable_job_can_be_claimed_again
    assert Stampeded::Lifecycle.legal?(:retryable, :claim)
    assert Stampeded::Lifecycle.claimable?(:retryable)
  end

  def test_a_finished_job_can_never_run_again
    %i[completed discarded cancelled].each do |s|
      refute Stampeded::Lifecycle.legal?(s, :claim), "#{s} must not be claimable"
      assert Stampeded::Lifecycle.terminal?(s)
      assert_equal(s == :completed ? [] : [:resurrect], Stampeded::Lifecycle.events_from(s),
                   "#{s} must be a dead end to everything but an operator")
    end
    assert_equal :available, Stampeded::Lifecycle.apply(:discarded, :resurrect)
    refute Stampeded::Lifecycle.terminal?(:retryable)
  end

  def test_an_executing_job_cannot_be_cancelled_from_under_its_worker
    refute Stampeded::Lifecycle.legal?(:executing, :cancel)
    assert Stampeded::Lifecycle.legal?(:available, :cancel)
    assert Stampeded::Lifecycle.legal?(:retryable, :cancel)
  end

  def test_only_a_running_job_can_complete
    assert Stampeded::Lifecycle.legal?(:executing, :complete)
    %i[available claimed retryable completed].each do |s|
      refute Stampeded::Lifecycle.legal?(s, :complete), "#{s} must not complete"
    end
  end

  def test_the_reaper_can_only_reclaim_jobs_that_were_held
    assert Stampeded::Lifecycle.legal?(:executing, :reclaim)
    assert Stampeded::Lifecycle.legal?(:claimed, :reclaim)
    refute Stampeded::Lifecycle.legal?(:available, :reclaim)
  end

  def test_a_job_given_back_unstarted_returns_to_the_queue
    assert_equal :available, Stampeded::Lifecycle.apply(:executing, :release)
    refute Stampeded::Lifecycle.legal?(:available, :release)
  end

  def test_the_states_and_events_are_the_libraries
    assert_equal %i[available claimed executing retryable completed discarded cancelled], Stampeded::Lifecycle.states
    assert_includes Stampeded::Lifecycle.events, :release
    refute Stampeded::Lifecycle.legal?(:running, :claim)
    assert_raises(Stampeded::Error) { Stampeded::Lifecycle.apply(:available, :explode) }
  end

  def test_a_job_answers_for_its_state
    job = Stampeded::Job.new({ "state" => "retryable" })
    assert job.retryable?
    refute job.completed?
    assert job.claimable?
    refute job.terminal?
    assert_includes job.available_events, :claim
    assert_raises(NoMethodError) { job.running? }
  end
end
