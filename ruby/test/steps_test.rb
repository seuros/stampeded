# frozen_string_literal: true

require "test_helper"

SIDE_EFFECTS = Queue.new

class SteppedJob < ActiveJob::Base
  def perform
    stampeded_step("charge") { SIDE_EFFECTS << :charged }

    path = stampeded_step("render") do
      SIDE_EFFECTS << :rendered
      "/tmp/receipt.pdf"
    end
    SIDE_EFFECTS << [:path, path]

    stampeded_step("email", cursor: 0) do |step|
      (step.cursor...3).each do |i|
        SIDE_EFFECTS << [:emailed, i]
        step.advance!(i + 1)
        raise "crash after email #{i}" if i == 1 && !stampeded_resumed?
      end
    end

    SIDE_EFFECTS << :done
  end
end

class StepsTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    SIDE_EFFECTS.clear
  end

  def drain(worker, timeout: 15, &done)
    t = Thread.new { worker.run }
    deadline = Time.now + timeout
    sleep 0.05 until done.call || Time.now > deadline
    worker.stop
    t.join(5)
  ensure
    t&.kill
  end

  def events
    out = []
    out << SIDE_EFFECTS.pop until SIDE_EFFECTS.empty?
    out
  end

  def test_a_completed_step_never_runs_twice
    # The point of the whole feature: a charge that succeeded must not be
    # charged again because a LATER step crashed.
    job = SteppedJob.perform_later
    drain(Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)) do
      job_row(job.provider_job_id)["state"] == "completed"
    end

    e = events
    assert_equal 1, e.count(:charged), "the charge must happen exactly once"
    assert_equal 1, e.count(:rendered), "and so must the render"
    assert_equal 1, e.count(:done)
  end

  def test_a_skipped_step_still_returns_what_it_produced
    # River skips the step and returns nothing, so `data = step { fetch }` is
    # nil on resume. Persisting small results is the departure that makes the
    # obvious usage correct.
    job = SteppedJob.perform_later
    drain(Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)) do
      job_row(job.provider_job_id)["state"] == "completed"
    end

    paths = events.select { |x| x.is_a?(Array) && x.first == :path }.map(&:last)
    assert_equal ["/tmp/receipt.pdf", "/tmp/receipt.pdf"], paths,
                 "the second attempt must see the first attempt's value, not nil"
  end

  def test_a_cursor_resumes_mid_step
    job = SteppedJob.perform_later
    drain(Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)) do
      job_row(job.provider_job_id)["state"] == "completed"
    end

    emailed = events.select { |x| x.is_a?(Array) && x.first == :emailed }.map(&:last)
    assert_equal [0, 1, 2], emailed, "each recipient exactly once, across the crash"
  end

  def test_a_duplicate_step_name_is_refused
    # Two steps with one name makes "have I done this?" ambiguous and the
    # fast-forward silently wrong. River detects this too.
    ctx = fake_context
    steps = Stampeded::Steps.new(ctx)
    steps.step("a") { 1 }
    assert_raises(Stampeded::Steps::DuplicateStep) { steps.step("a") { 2 } }
  end

  def test_a_large_step_result_is_refused_with_advice
    # Silently dropping it would give a job that works until the first crash
    # and then behaves differently.
    steps = Stampeded::Steps.new(fake_context)
    error = assert_raises(Stampeded::Steps::ResultTooLarge) do
      steps.step("big") { "x" * 20_000 }
    end
    assert_match(/reference to it/, error.message)
  end

  def test_a_nil_result_is_not_stored
    # The common case is a step run purely for its side effect; storing nil for
    # every one of them would bloat the state for nothing.
    ctx = fake_context
    steps = Stampeded::Steps.new(ctx)
    steps.step("side_effect") { nil }
    assert_empty ctx.saved.last["values"]
    assert_equal ["side_effect"], ctx.saved.last["completed"]
  end

  # A JobContext stand-in that records what was persisted.
  def fake_context
    Class.new do
      attr_reader :saved

      def initialize = @saved = []
      def checkpoint = @saved.last
      def checkpoint!(state, conn: nil) = @saved << state
    end.new
  end
end
