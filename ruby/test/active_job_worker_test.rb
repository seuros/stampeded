# frozen_string_literal: true

require "test_helper"

RESULTS = Queue.new

class EndToEndJob < ActiveJob::Base
  def perform(who) = RESULTS << "hello #{who}"
end

class FailingEndToEndJob < ActiveJob::Base
  def perform = raise(ArgumentError, "bad input")
end

class DiscardingJob < ActiveJob::Base
  discard_on ArgumentError
  def perform = raise(ArgumentError, "discarded, not failed")
end

class AttemptReportingJob < ActiveJob::Base
  stampeded max_attempts: 3, backoff_base_ms: 1

  def perform
    RESULTS << [stampeded_attempt, executions]
    raise "again" if stampeded_attempt < 2
  end
end

class ActiveJobWorkerTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    RESULTS.clear
  end

  def run_worker(worker, timeout: 10, &done)
    thread = Thread.new { worker.run }
    deadline = Time.now + timeout
    sleep 0.05 until done.call || Time.now > deadline
    worker.stop
    thread.join(5)
  ensure
    thread&.kill
  end

  def test_perform_later_is_actually_executed_by_the_ruby_worker
    job = EndToEndJob.perform_later("world")

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { !RESULTS.empty? }

    assert_equal "hello world", RESULTS.pop
    assert_equal "completed", job_row(job.provider_job_id)["state"]
  end

  def test_stampeded_attempt_counts_where_executions_cannot
    job = AttemptReportingJob.perform_later

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(job.provider_job_id)["state"] == "completed" }

    assert_equal [[1, 1], [2, 1]], Array.new(RESULTS.size) { RESULTS.pop }
    assert_nil AttemptReportingJob.new.stampeded_attempt, "nil outside a worker"
  end

  def test_an_unhandled_exception_becomes_a_stampeded_failure
    job = FailingEndToEndJob.perform_later

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(job.provider_job_id)["state"] == "retryable" }

    row = job_row(job.provider_job_id)
    assert_equal "raised", row["failure_kind"]
    assert_match(/bad input/, row["last_error"])
  end

  def test_discard_on_completes_the_job_rather_than_failing_it
    job = DiscardingJob.perform_later

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(job.provider_job_id)["state"] != "available" }

    assert_equal "completed", job_row(job.provider_job_id)["state"]
  end

  def test_a_job_class_that_no_longer_exists_is_unrecoverable
    id = Stampeded.enqueue(
      queue: "default", task: "DeletedJob",
      args: { "job_class" => "DeletedJob", "job_id" => SecureRandom.uuid,
              "queue_name" => "default", "arguments" => [], "executions" => 0,
              "priority" => nil, "locale" => "en" }
    )

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(id)["state"] == "discarded" }

    row = job_row(id)
    assert_equal "discarded", row["state"]
    assert_equal "unrecoverable", row["failure_kind"]
  end
end

class NoMethodJob < ActiveJob::Base
  def perform = nil.this_method_does_not_exist
end

class MisclassificationTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
  end

  def test_a_nomethoderror_inside_a_job_is_retried_not_discarded
    job = NoMethodJob.perform_later(*[])
    id = job.provider_job_id

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    t = Thread.new { worker.run }
    deadline = Time.now + 10
    sleep 0.05 until job_row(id)["state"] != "available" || Time.now > deadline
    worker.stop
    t.join(5)

    row = job_row(id)
    assert_equal "retryable", row["state"], "a bug in a job is retryable, not terminal"
    assert_equal "raised", row["failure_kind"], "and it is a raise, not an unknown task"
    assert_match(/this_method_does_not_exist/, row["last_error"])
  ensure
    t&.kill
  end

  def test_a_genuinely_missing_job_class_is_still_unrecoverable
    id = Stampeded.enqueue(
      queue: "default", task: "VanishedJob",
      args: { "job_class" => "VanishedJob", "job_id" => SecureRandom.uuid,
              "queue_name" => "default", "arguments" => [], "executions" => 0,
              "priority" => nil, "locale" => "en" }
    )

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    t = Thread.new { worker.run }
    deadline = Time.now + 10
    sleep 0.05 until job_row(id)["state"] == "discarded" || Time.now > deadline
    worker.stop
    t.join(5)

    row = job_row(id)
    assert_equal "discarded", row["state"]
    assert_equal "unrecoverable", row["failure_kind"]
    assert_match(/VanishedJob is not defined/, row["last_error"])
  ensure
    t&.kill
  end
end
