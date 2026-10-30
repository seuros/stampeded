# frozen_string_literal: true

require "test_helper"

class QueuePolicyTest < Minitest::Test
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

  def verdict!(queue, verdict, age: 0)
    conn do |c|
      c.exec_params(<<~SQL, [queue, verdict, age])
        UPDATE stampeded_queue_policies
           SET verdict = $2, decided_at = now() - make_interval(secs => $3::int)
         WHERE queue = $1
      SQL
    end
  end

  def test_policies_are_set_updated_and_removed
    Stampeded::QueuePolicy.set("extraction", max_running: 1)
    Stampeded::QueuePolicy.set("mail", max_wait: 60)
    Stampeded::QueuePolicy.set("mail", max_wait: 30)

    rows = Stampeded::QueuePolicy.all.to_h { [it["queue"], it] }
    assert_equal "1", rows["extraction"]["max_running"]
    assert_equal "30", rows["mail"]["max_wait_secs"]
    assert_equal "steady", rows["mail"]["verdict"]

    Stampeded::QueuePolicy.remove("mail")
    assert_equal ["extraction"], Stampeded::QueuePolicy.all.map { it["queue"] }
  end

  def test_a_held_queue_is_not_claimed
    Stampeded::QueuePolicy.set("held")
    verdict!("held", "hold")
    held = Stampeded.enqueue(queue: "held", task: "note")
    open = Stampeded.enqueue(queue: "open", task: "note")

    worker = Stampeded::Worker.new(queues: %w[held open], poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(open)["state"] == "completed" }
    sleep 0.2

    assert_equal "available", job_row(held)["state"]
  end

  def test_a_paused_queue_waits_until_resumed_with_no_daemon
    Stampeded::QueuePolicy.pause("frozen")
    Stampeded::QueuePolicy.set("frozen", max_running: 5)
    frozen = Stampeded.enqueue(queue: "frozen", task: "note")
    open = Stampeded.enqueue(queue: "open", task: "note")

    worker = Stampeded::Worker.new(queues: %w[frozen open], poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(open)["state"] == "completed" }
    sleep 0.2

    assert_equal "available", job_row(frozen)["state"]
    assert_equal "t", Stampeded::QueuePolicy.all.find { it["queue"] == "frozen" }["paused"],
                 "set keeps a pause"

    Stampeded::QueuePolicy.resume("frozen")
    worker = Stampeded::Worker.new(queues: %w[frozen], poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(frozen)["state"] == "completed" }
  end

  def test_a_stale_hold_is_ignored
    Stampeded::QueuePolicy.set("held")
    verdict!("held", "hold", age: 60)
    held = Stampeded.enqueue(queue: "held", task: "note")

    worker = Stampeded::Worker.new(queues: %w[held], poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(held)["state"] == "completed" }

    assert_equal "completed", job_row(held)["state"]
  end

  def test_a_rushed_queue_is_claimed_first
    3.times { Stampeded.enqueue(queue: "bulk", task: "note", args: { "q" => "bulk" }) }
    Stampeded.enqueue(queue: "late", task: "note", args: { "q" => "late" })
    Stampeded::QueuePolicy.set("late", max_wait: 60)
    verdict!("late", "rush")

    seen = []
    lock = Mutex.new
    worker = Stampeded::Worker.new(queues: %w[bulk late], concurrency: 1, batch_size: 1,
                                   poll_interval: 0.05)
    worker.register("note") { |job| lock.synchronize { seen << job.args["q"] } }
    run_worker(worker) { lock.synchronize { seen.size >= 4 } }

    assert_equal "late", seen.first
  end

  def test_max_running_caps_this_process_without_a_daemon
    Stampeded::QueuePolicy.set("one", max_running: 1)
    4.times { Stampeded.enqueue(queue: "one", task: "slow") }

    running = 0
    peak = 0
    done = 0
    lock = Mutex.new
    worker = Stampeded::Worker.new(queues: %w[one], concurrency: 4, poll_interval: 0.05)
    worker.register("slow") do
      lock.synchronize { running += 1; peak = [peak, running].max }
      sleep 0.1
      lock.synchronize { running -= 1; done += 1 }
    end
    run_worker(worker, timeout: 15) { lock.synchronize { done >= 4 } }

    assert_equal 1, peak, "ran #{peak} at once from a queue capped at 1"
  end

  def test_a_schema_without_the_policies_table_still_works
    conn { |c| c.exec("DROP TABLE stampeded_queue_policies") }
    id = Stampeded.enqueue(queue: "default", task: "note")

    worker = Stampeded::Worker.new(queues: %w[default], poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(id)["state"] == "completed" }

    assert_equal "completed", job_row(id)["state"]
  end
end
