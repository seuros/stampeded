# frozen_string_literal: true

require "test_helper"

# The transition table, in Ruby. Each of these is a specific bug that a
# string-comparison lifecycle lets through by accident -- and the last one
# checks that Ruby and Rust declare the SAME table, which is the whole point of
# declaring it at all.
class LifecycleTest < Minitest::Test
  def test_a_retryable_job_can_be_claimed_again
    # THE poison-job bug. If retryable were not claimable, a crashed job would
    # park there forever, never spend its attempt budget, and never reach
    # max_attempts -- an unbounded retry loop that looks like a stalled queue.
    assert Stampeded::Lifecycle.legal?(:retryable, :claim)
    assert Stampeded::Lifecycle.claimable?(:retryable)
  end

  def test_a_finished_job_can_never_run_again
    # Re-running a completed job is the worst bug a queue can have: it charges
    # the card twice.
    %i[completed discarded cancelled].each do |s|
      refute Stampeded::Lifecycle.legal?(s, :claim), "#{s} must not be claimable"
      assert Stampeded::Lifecycle.terminal?(s)
      assert_empty Stampeded::Lifecycle.events_from(s), "#{s} must be a dead end"
    end
  end

  def test_an_executing_job_cannot_be_cancelled_from_under_its_worker
    # The worker owns it. Flipping the row would not stop the work; it would
    # only make the worker's completion fail its ownership check.
    refute Stampeded::Lifecycle.legal?(:executing, :cancel)
    assert Stampeded::Lifecycle.legal?(:available, :cancel)
    assert Stampeded::Lifecycle.legal?(:retryable, :cancel)
  end

  def test_only_a_running_job_can_complete
    # `available -> completed` would mean reporting success for work never
    # done. Reachable in any implementation that compares strings and forgets
    # one of them.
    assert Stampeded::Lifecycle.legal?(:executing, :complete)
    %i[available claimed retryable completed].each do |s|
      refute Stampeded::Lifecycle.legal?(s, :complete), "#{s} must not complete"
    end
  end

  def test_the_reaper_can_only_reclaim_jobs_that_were_held
    # Reclaiming an available job would charge an interruption to a job nobody
    # was running.
    assert Stampeded::Lifecycle.legal?(:executing, :reclaim)
    assert Stampeded::Lifecycle.legal?(:claimed, :reclaim)
    refute Stampeded::Lifecycle.legal?(:available, :reclaim)
  end

  def test_verify_accepts_what_sql_really_does_and_rejects_what_it_should_not
    assert Stampeded::Lifecycle.verify(:executing, :completed, :complete)
    assert Stampeded::Lifecycle.verify(:retryable, :executing, :claim)
    refute Stampeded::Lifecycle.verify(:executing, :available, :complete),
           "executing -> available is not a completion"
  end

  def test_ruby_and_rust_declare_the_same_lifecycle
    # The reason to declare it at all: two first-party workers that disagree
    # about what a job may do would be worse than two that both guess.
    rust = File.read(File.expand_path("../../crates/stampeded/src/lifecycle.rs", __dir__))

    # States.
    declared = rust[/states:\s*\[([^\]]+)\]/, 1].split(",").map { |s| s.strip.downcase.to_sym }
    assert_equal Stampeded::Lifecycle::STATES.sort, declared.sort,
                 "Ruby and Rust disagree about the set of job states"

    # Events, and which states each is legal from.
    %w[claim complete retry discard cancel reclaim].each do |event|
      body = rust[/^\s+#{event} \{(.*?)\n\s+\}/m, 1]
      refute_nil body, "Rust declares no #{event} event"

      rust_from = body.scan(/from:\s*(\w+)/).flatten.map { |s| s.downcase.to_sym }.sort.uniq
      ruby_from = Stampeded::Lifecycle::STATES.select do |s|
        Stampeded::Lifecycle.legal?(s, event)
      end.sort

      assert_equal rust_from, ruby_from,
                   "#{event}: Rust allows it from #{rust_from.inspect}, Ruby from #{ruby_from.inspect}"
    end
  end
end
