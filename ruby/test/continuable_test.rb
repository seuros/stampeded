# frozen_string_literal: true

require "test_helper"
require "active_job/continuable"

class ContinuedImportJob < ActiveJob::Base
  include ActiveJob::Continuable

  stampeded max_attempts: 5, backoff_base_ms: 1

  class << self
    attr_accessor :log, :on_row
  end

  def perform(total)
    step :prepare do
      self.class.log << [:prepare, resumptions]
    end

    step :rows, start: 0 do |rows|
      (rows.cursor...total).each do |i|
        self.class.log << [:row, i, resumptions]
        self.class.on_row&.call(i)
        rows.advance!(from: i)
      end
    end
  end
end

class IsolatedStepJob < ActiveJob::Base
  include ActiveJob::Continuable

  class << self
    attr_accessor :log
  end

  def perform
    step(:first) { self.class.log << [:first, resumptions] }
    step(:alone, isolated: true) { self.class.log << [:alone, resumptions] }
  end
end

class ContinuableTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    ContinuedImportJob.log = Queue.new
    ContinuedImportJob.on_row = nil
    IsolatedStepJob.log = Queue.new
  end

  def start(worker) = Thread.new { worker.run }

  def worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)

  def wait_for(timeout: 10)
    deadline = Time.now + timeout
    sleep 0.05 until yield || Time.now > deadline
    assert yield, "timed out"
  end

  def drain(queue) = Array.new(queue.size) { queue.pop }

  def rows = conn { |c| c.exec("SELECT id, state, attempt, failure_kind, checkpoint FROM stampeded_jobs").to_a }

  def test_a_stopping_worker_interrupts_and_the_same_row_resumes_where_it_stopped
    reached = Queue.new
    proceed = Queue.new
    ContinuedImportJob.on_row = ->(i) { (reached << i) && proceed.pop if i == 2 }
    job = ContinuedImportJob.perform_later(5)

    first = worker
    running = start(first)
    assert_equal 2, reached.pop
    refute ActiveJob::Base.queue_adapter.stopping?
    first.stop
    assert ActiveJob::Base.queue_adapter.stopping?, "the adapter reports the worker stopping"
    proceed << true
    running.join(10)

    row = job_row(job.provider_job_id)
    assert_equal 1, rows.size, "resumed in place, not re-enqueued as a new job"
    assert_equal "available", row["state"]
    assert_equal "0", row["attempt"], "a shutdown is not an attempt"
    saved = JSON.parse(row["checkpoint"])["activejob"]
    assert_equal({ "completed" => ["prepare"], "current" => ["rows", 3] }, saved["continuation"])
    assert_equal [[:prepare, 0], [:row, 0, 0], [:row, 1, 0], [:row, 2, 0]], drain(ContinuedImportJob.log)

    ContinuedImportJob.on_row = nil
    second = worker
    running = start(second)
    wait_for { job_row(job.provider_job_id)["state"] == "completed" }
    second.stop
    running.join(10)

    assert_equal [[:row, 3, 1], [:row, 4, 1]], drain(ContinuedImportJob.log), "prepare skipped, rows from the cursor"
  end

  def test_an_error_after_progress_is_an_attempt_and_the_next_one_resumes
    failed = false
    ContinuedImportJob.on_row = lambda do |i|
      next unless i == 2 && !failed

      failed = true
      raise ArgumentError, "row 2 is bad"
    end
    job = ContinuedImportJob.perform_later(4)

    w = worker
    running = start(w)
    wait_for { job_row(job.provider_job_id)["state"] == "completed" }
    w.stop
    running.join(10)

    row = job_row(job.provider_job_id)
    assert_equal 1, rows.size
    assert_equal "2", row["attempt"], "the failure was charged"
    log = drain(ContinuedImportJob.log)
    assert_equal [[:prepare, 0], [:row, 0, 0], [:row, 1, 0], [:row, 2, 0], [:row, 2, 1], [:row, 3, 1]], log
  end

  def test_an_isolated_step_runs_in_its_own_execution_of_the_same_row
    job = IsolatedStepJob.perform_later

    w = worker
    running = start(w)
    wait_for { job_row(job.provider_job_id)["state"] == "completed" }
    w.stop
    running.join(10)

    assert_equal 1, rows.size
    assert_equal "1", job_row(job.provider_job_id)["attempt"], "the isolating requeue refunded its attempt"
    assert_equal [[:first, 0], [:alone, 1]], drain(IsolatedStepJob.log)
  end

  def test_max_resumptions_still_applies
    ContinuedImportJob.max_resumptions = 0
    reached = Queue.new
    proceed = Queue.new
    ContinuedImportJob.on_row = ->(i) { (reached << i) && proceed.pop if i == 1 }
    job = ContinuedImportJob.perform_later(3)

    w = worker
    running = start(w)
    reached.pop
    w.stop
    proceed << true
    running.join(10)

    row = job_row(job.provider_job_id)
    assert_equal "retryable", row["state"]
    assert_equal "raised", row["failure_kind"]
  ensure
    ContinuedImportJob.max_resumptions = nil
  end
end
