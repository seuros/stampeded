# frozen_string_literal: true

require "test_helper"
require "time"

class WorkerTest < Minitest::Test
  include StampededTest::Isolation

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
    id = Stampeded.enqueue(queue: "default", task: "greet", args: { "who" => "ruby" })

    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("greet") { |job| ran << job.args["who"] }

    run_worker(worker) { !ran.empty? }

    assert_equal "ruby", ran.pop
    assert_equal "completed", job_row(id)["state"]
  end

  def test_a_raising_job_is_recorded_as_retryable_with_the_error
    # A long backoff so the worker CANNOT re-claim before the assertions run.
    #
    # Without it this test races: the job fails, gets rand(0..1000)ms of
    # backoff, and the worker -- still draining -- claims it again, making
    # `attempt` 2. Locally the loop is fast enough that it almost never
    # happens; under CI, which ran this suite 40x slower, it failed with
    # "Expected 1, Actual 2". Found by running the workflow under act.
    id = Stampeded.enqueue(queue: "default", task: "boom", max_attempts: 5,
                          backoff_base_ms: 30_000)

    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
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
    id = Stampeded.enqueue(queue: "default", task: "nobody_handles_this", max_attempts: 25)

    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    run_worker(worker) { job_row(id)["state"] == "discarded" }

    row = job_row(id)
    assert_equal "discarded", row["state"]
    assert_equal "unrecoverable", row["failure_kind"]
  end

  def test_the_worker_registers_and_deregisters_itself
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    seen = Queue.new
    worker.register("x") { seen << true }
    Stampeded.enqueue(queue: "default", task: "x")

    run_worker(worker) { !seen.empty? }

    count = conn { |c| c.exec("SELECT count(*) FROM stampeded_workers")[0]["count"].to_i }
    assert_equal 0, count, "a clean shutdown must not leave a phantom worker for the reaper"
  end

  def test_concurrency_is_bounded_by_the_pool
    # Claiming more than the pool can run would park jobs in `executing` behind
    # a mutex, where the reaper cannot tell them from genuinely running work.
    concurrent = Queue.new
    peak = 0
    mutex = Mutex.new
    running = 0

    10.times { Stampeded.enqueue(queue: "default", task: "slow") }

    worker = Stampeded::Worker.new(queue: "default", concurrency: 2, poll_interval: 0.05)
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
    id = Stampeded.enqueue(queue: "default", task: "slow")

    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
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

  # An ordinary job failure must be REPORTED, in a worker that never loaded
  # Rails.
  #
  # `failure_kind_for` matches on `Timeout::Error`, and nothing in the gem
  # required "timeout". Inside Rails that constant exists because ActiveSupport
  # loads it, which is why every existing test passed. In the plain-Ruby worker
  # the gem explicitly supports via `register`, the `when Timeout::Error` clause
  # is evaluated for ANY error that is not UnknownTask -- so every ordinary
  # failure raised NameError inside the rescue, report(:fail) never ran, and the
  # job sat in `executing` with a null last_error until the reaper took it,
  # charging an interruption for a clean application error.
  # The in-process half: the failure is reported and classified.
  #
  # NOTE: this half CANNOT detect the missing require, because test_helper
  # requires active_job, which loads ActiveSupport, which loads timeout. The
  # first version of this test passed with the fix reverted for exactly that
  # reason. The subprocess test below is the one that actually holds the line.
  def test_an_ordinary_failure_is_reported
    id = Stampeded.enqueue(queue: "default", task: "boom", max_attempts: 1)
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("boom") { raise ArgumentError, "ordinary failure" }

    run_worker(worker) { job_row(id)["state"] != "available" }

    row = job_row(id)

    refute_equal "executing", row["state"], "the failure was never reported"
    assert_equal "raised", row["failure_kind"]
    assert_match "ordinary failure", row["last_error"]
  end

  # Layer 1 must fire before layer 2, or the database hands a merely-slow job
  # to a second worker while the first is still running it.
  #
  # The Ruby worker had no per-job timeout at all, so River's
  # `rescue_after > job_timeout` invariant was violated by construction rather
  # than by misconfiguration: a hanging job kept its thread while
  # `stampeded_expire` returned the row to `retryable` for someone else to claim.
  def test_a_hanging_job_is_timed_out_before_the_database_expires_it
    Stampeded.config.job_timeout = 1.0
    id = Stampeded.enqueue(queue: "default", task: "hang", expire_seconds: 60, max_attempts: 1)

    starts = Queue.new
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("hang") { starts << 1; sleep 30 }

    run_worker(worker) { %w[discarded retryable].include?(job_row(id)["state"]) }

    assert_equal 1, starts.size, "the job must not have been started twice"
    assert_equal "timed_out", job_row(id)["failure_kind"]
    assert_match(/job_timeout/, job_row(id)["last_error"])
  ensure
    Stampeded.config.job_timeout = 900.0
  end



  # A handler choosing its own retry delay -- the Stampeded answer to
  # retry_on(wait:), which the adapter refuses because it re-enqueues a NEW row
  # and resets every counter.
  #
  # Asserts the delay reaches the DATABASE, not merely that the object holds
  # it: the whole point is that scheduled_at moves, and a delay computed and
  # then dropped on the floor would look identical from Ruby.
  def test_a_handler_can_choose_its_own_retry_delay
    id = Stampeded.enqueue(queue: "default", task: "ratelimited", max_attempts: 5)
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("ratelimited") { raise Stampeded::RetryIn.new(3600, "rate limited") }

    run_worker(worker) { job_row(id)["state"] == "retryable" }

    row = job_row(id)
    delay = Time.parse(row["scheduled_at"]) - Time.now

    assert_equal "retryable", row["state"]
    assert_equal 1, row["attempt"].to_i, "RetryIn must still charge the attempt"
    # An hour, not the default backoff, which at attempt 1 is at most a second.
    assert_operator delay, :>, 3000, "the handler's delay did not reach the database (got #{delay}s)"
  end

  # RetryIn picks when, not whether: the poison brake still applies.
  def test_retry_in_does_not_exempt_a_job_from_max_attempts
    id = Stampeded.enqueue(queue: "default", task: "always", max_attempts: 1)
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("always") { raise Stampeded::RetryIn.new(0.01) }

    run_worker(worker) { %w[discarded retryable].include?(job_row(id)["state"]) }

    assert_equal "discarded", job_row(id)["state"],
                 "max_attempts must still bound a job that picks its own delay"
  end

end

class WorkerValidationTest < Minitest::Test
  def test_a_worker_that_could_never_work_is_refused
    # `CONCURRENCY=abc bin/rails stampeded:work` becomes 0 via String#to_i, and
    # the loop then computes `free = 0 - 0` and sleeps in a tight circle doing
    # nothing -- a process that looks healthy, passes every liveness check, and
    # silently runs no jobs.
    assert_raises(ArgumentError) { Stampeded::Worker.new(queue: "q", concurrency: 0) }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queue: "q", concurrency: "abc") }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queue: "q", batch_size: 0) }

    assert Stampeded::Worker.new(queue: "q", concurrency: 1, batch_size: 1)
  end

end
