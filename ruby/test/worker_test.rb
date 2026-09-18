# frozen_string_literal: true

require "test_helper"

class WorkerTest < Minitest::Test
  include StampedeTest::Isolation

  # Run a worker until `until_done` is satisfied, then stop it. Bounded so a
  # broken worker fails the test instead of hanging the suite.
  def run_worker(worker, timeout: 10, &until_done)
    thread = Thread.new { worker.run }
    deadline = Time.now + timeout
    sleep 0.05 until until_done.call || Time.now > deadline
    worker.stop
    thread.join(5)
    refute_operator Time.now, :>, deadline, "worker did not finish in #{timeout}s"
  ensure
    thread&.kill
  end

  def test_a_job_is_claimed_executed_and_completed
    ran = Queue.new
    id = Stampede.enqueue(queue: "default", task: "greet", args: { "who" => "ruby" })

    worker = Stampede::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("greet") { |job| ran << job.args["who"] }

    run_worker(worker) { !ran.empty? }

    assert_equal "ruby", ran.pop
    assert_equal "completed", job_row(id)["state"]
  end

  def test_a_raising_job_is_recorded_as_retryable_with_the_error
    id = Stampede.enqueue(queue: "default", task: "boom", max_attempts: 5)

    worker = Stampede::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("boom") { raise "the downstream exploded" }

    run_worker(worker) { job_row(id)["state"] == "retryable" }

    row = job_row(id)
    assert_equal "retryable", row["state"]
    assert_equal "raised", row["failure_kind"]
    assert_match(/downstream exploded/, row["last_error"])
    assert_equal "1", row["attempt"], "the attempt was spent at CLAIM, not at failure"
  end

  def test_an_unknown_task_is_unrecoverable_rather_than_retried_forever
    # A job enqueued for a task no worker handles: a rename deployed to the
    # enqueuer but not the workers. Retrying cannot help.
    id = Stampede.enqueue(queue: "default", task: "nobody_handles_this", max_attempts: 25)

    worker = Stampede::Worker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(id)["state"] == "discarded" }

    row = job_row(id)
    assert_equal "discarded", row["state"]
    assert_equal "unrecoverable", row["failure_kind"]
  end

  def test_the_worker_registers_and_deregisters_itself
    worker = Stampede::Worker.new(queue: "default", poll_interval: 0.05)
    seen = Queue.new
    worker.register("x") { seen << true }
    Stampede.enqueue(queue: "default", task: "x")

    run_worker(worker) { !seen.empty? }

    count = conn { |c| c.exec("SELECT count(*) FROM stampede_workers")[0]["count"].to_i }
    assert_equal 0, count, "a clean shutdown must not leave a phantom worker for the reaper"
  end

  def test_concurrency_is_bounded_by_the_pool
    # Claiming more than the pool can run would park jobs in `executing` behind
    # a mutex, where the reaper cannot tell them from genuinely running work.
    concurrent = Queue.new
    peak = 0
    mutex = Mutex.new
    running = 0

    10.times { Stampede.enqueue(queue: "default", task: "slow") }

    worker = Stampede::Worker.new(queue: "default", concurrency: 2, poll_interval: 0.05)
    worker.register("slow") do
      mutex.synchronize { running += 1; peak = [peak, running].max }
      sleep 0.15
      mutex.synchronize { running -= 1 }
      concurrent << true
    end

    run_worker(worker, timeout: 20) { concurrent.size >= 6 }

    assert_operator peak, :<=, 2, "ran #{peak} jobs at once with concurrency 2"
  end

  def test_a_graceful_shutdown_does_not_charge_an_interruption
    # plans/15: a routine deploy must not consume the interruption budget. The
    # drain is what achieves that -- the job reports its own outcome normally,
    # so nothing is ever recorded as a worker death.
    started = Queue.new
    id = Stampede.enqueue(queue: "default", task: "slow")

    worker = Stampede::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("slow") do
      started << true
      sleep 0.4
    end

    thread = Thread.new { worker.run }
    started.pop                 # in-flight
    worker.stop                 # deploy
    thread.join(10)

    row = job_row(id)
    assert_equal "completed", row["state"], "the in-flight job should have been allowed to finish"
    assert_equal "0", row["interruptions"], "a graceful shutdown is not a worker death"
  ensure
    thread&.kill
  end
end

class WorkerValidationTest < Minitest::Test
  def test_a_worker_that_could_never_work_is_refused
    # `CONCURRENCY=abc bin/rails stampede:work` becomes 0 via String#to_i, and
    # the loop then computes `free = 0 - 0` and sleeps in a tight circle doing
    # nothing -- a process that looks healthy, passes every liveness check, and
    # silently runs no jobs.
    assert_raises(ArgumentError) { Stampede::Worker.new(queue: "q", concurrency: 0) }
    assert_raises(ArgumentError) { Stampede::Worker.new(queue: "q", concurrency: "abc") }
    assert_raises(ArgumentError) { Stampede::Worker.new(queue: "q", batch_size: 0) }

    assert Stampede::Worker.new(queue: "q", concurrency: 1, batch_size: 1)
  end
end
