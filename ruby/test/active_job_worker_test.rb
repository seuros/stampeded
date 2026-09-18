# frozen_string_literal: true

require "test_helper"

# Results land here because the worker runs the job in another thread.
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

class ActiveJobWorkerTest < Minitest::Test
  include StampedeTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampedeAdapter.new
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
    # The whole point of plans/16: the Rust daemon cannot run this, because an
    # ActiveJob job is a Ruby class. The Ruby worker claims through the same
    # SQL function the Rust worker uses and then hands off to ActiveJob.
    job = EndToEndJob.perform_later("world")

    worker = Stampede::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { !RESULTS.empty? }

    assert_equal "hello world", RESULTS.pop
    assert_equal "completed", job_row(job.provider_job_id)["state"]
  end

  def test_an_unhandled_exception_becomes_a_stampede_failure
    job = FailingEndToEndJob.perform_later

    worker = Stampede::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(job.provider_job_id)["state"] == "retryable" }

    row = job_row(job.provider_job_id)
    assert_equal "raised", row["failure_kind"]
    assert_match(/bad input/, row["last_error"])
  end

  def test_discard_on_completes_the_job_rather_than_failing_it
    # ActiveJob swallows it, so nothing reaches us and the job is done. This is
    # why discard_on needs no special handling while retry_on is refused.
    job = DiscardingJob.perform_later

    worker = Stampede::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(job.provider_job_id)["state"] != "available" }

    assert_equal "completed", job_row(job.provider_job_id)["state"]
  end

  def test_a_job_class_that_no_longer_exists_is_unrecoverable
    # A rename deployed to the enqueuer but not to the workers. No amount of
    # retrying brings the constant back, so it must not burn 25 attempts.
    id = Stampede.enqueue(
      queue: "default", task: "DeletedJob",
      args: { "job_class" => "DeletedJob", "job_id" => SecureRandom.uuid,
              "queue_name" => "default", "arguments" => [], "executions" => 0,
              "priority" => nil, "locale" => "en" }
    )

    worker = Stampede::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
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
  include StampedeTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampedeAdapter.new
  end

  def test_a_nomethoderror_inside_a_job_is_retried_not_discarded
    # NoMethodError IS A NameError. Rescuing NameError around the whole
    # execution -- to detect a deleted job class -- misclassified every `nil.foo`
    # typo as an unknown task and DISCARDED the job without a single retry.
    # A transient nil during a deploy would have silently destroyed work.
    job = NoMethodJob.perform_later(*[])
    id = job.provider_job_id

    worker = Stampede::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
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
    # The case the rescue was there for must keep working.
    id = Stampede.enqueue(
      queue: "default", task: "VanishedJob",
      args: { "job_class" => "VanishedJob", "job_id" => SecureRandom.uuid,
              "queue_name" => "default", "arguments" => [], "executions" => 0,
              "priority" => nil, "locale" => "en" }
    )

    worker = Stampede::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
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
