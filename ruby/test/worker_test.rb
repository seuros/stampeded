# frozen_string_literal: true

require "test_helper"
require "time"

class WorkerTest < Minitest::Test
  include StampededTest::Isolation

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
    started = Queue.new
    id = Stampeded.enqueue(queue: "default", task: "slow")

    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("slow") do
      started << true
      sleep 0.4
    end

    thread = Thread.new { worker.run }
    started.pop
    worker.stop
    thread.join(10)

    row = job_row(id)
    assert_equal "completed", row["state"], "the in-flight job should have been allowed to finish"
    assert_equal "0", row["interruptions"], "a graceful shutdown is not a worker death"
  ensure
    thread&.kill
  end

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



  def test_a_handler_can_choose_its_own_retry_delay
    id = Stampeded.enqueue(queue: "default", task: "ratelimited", max_attempts: 5)
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("ratelimited") { raise Stampeded::RetryIn.new(3600, "rate limited") }

    run_worker(worker) { job_row(id)["state"] == "retryable" }

    row = job_row(id)
    delay = Time.parse(row["scheduled_at"]) - Time.now

    assert_equal "retryable", row["state"]
    assert_equal 1, row["attempt"].to_i, "RetryIn must still charge the attempt"
    assert_operator delay, :>, 3000, "the handler's delay did not reach the database (got #{delay}s)"
  end

  def test_retry_in_does_not_exempt_a_job_from_max_attempts
    id = Stampeded.enqueue(queue: "default", task: "always", max_attempts: 1)
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("always") { raise Stampeded::RetryIn.new(0.01) }

    run_worker(worker) { %w[discarded retryable].include?(job_row(id)["state"]) }

    assert_equal "discarded", job_row(id)["state"],
                 "max_attempts must still bound a job that picks its own delay"
  end

  def test_one_worker_runs_every_queue_it_is_given
    Stampeded.create_queue("mail")
    ran = Queue.new
    ids = [Stampeded.enqueue(queue: "default", task: "note", args: { "q" => "default" }),
           Stampeded.enqueue(queue: "mail", task: "note", args: { "q" => "mail" }),
           Stampeded.enqueue(queue: "unprovisioned", task: "note", args: { "q" => "unprovisioned" })]

    worker = Stampeded::Worker.new(queues: "default,mail,unprovisioned", poll_interval: 0.05)
    worker.register("note") { |job| ran << job.args["q"] }

    run_worker(worker) { ran.size >= 3 }

    assert_equal %w[default mail unprovisioned], Array.new(ran.size) { ran.pop }.sort
    assert(ids.all? { |id| job_row(id)["state"] == "completed" })
  end

  def test_a_deep_queue_does_not_starve_the_others
    20.times { Stampeded.enqueue(queue: "default", task: "note", args: { "q" => "default" }) }
    Stampeded.enqueue(queue: "mail", task: "note", args: { "q" => "mail" })

    seen = []
    lock = Mutex.new
    worker = Stampeded::Worker.new(queues: %w[default mail], concurrency: 1, batch_size: 1,
                                   poll_interval: 0.05)
    worker.register("note") { |job| lock.synchronize { seen << job.args["q"] } }

    run_worker(worker) { lock.synchronize { seen.include?("mail") } }

    assert_operator seen.index("mail"), :<=, 2, "mail waited behind #{seen.index("mail")} default jobs"
  end

  def test_the_worker_registers_every_queue
    seen = Queue.new
    worker = Stampeded::Worker.new(queues: %w[default mail], poll_interval: 0.05)
    worker.register("x") { seen << conn { |c| c.exec("SELECT queues FROM stampeded_workers")[0]["queues"] } }
    Stampeded.enqueue(queue: "mail", task: "x")

    run_worker(worker) { !seen.empty? }

    assert_equal "{default,mail}", seen.pop
  end
end

class WorkerValidationTest < Minitest::Test
  def test_a_worker_that_could_never_work_is_refused
    assert_raises(ArgumentError) { Stampeded::Worker.new(queue: "q", concurrency: 0) }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queue: "q", concurrency: "abc") }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queue: "q", batch_size: 0) }

    assert Stampeded::Worker.new(queue: "q", concurrency: 1, batch_size: 1)
  end

  def test_a_blank_queue_name_is_refused
    assert_raises(ArgumentError) { Stampeded::Worker.new(queues: "default,,mail") }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queues: []) }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queues: " ") }

    assert_equal %w[default mail], Stampeded::Worker.new(queues: " default , mail,default").queues
  end

  def test_queues_fall_back_to_config_then_default_queue
    assert_equal ["default"], Stampeded::Worker.new.queues

    Stampeded.config.queues = "a,b"
    assert_equal %w[a b], Stampeded::Worker.new.queues
    assert_equal ["c"], Stampeded::Worker.new(queue: "c").queues
  ensure
    Stampeded.config.queues = nil
  end

end
