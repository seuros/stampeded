# frozen_string_literal: true

require "test_helper"

class AllQueuesTest < Minitest::Test
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

  def test_every_queue_with_work_is_served_without_being_named
    ids = %w[alpha beta gamma].map { Stampeded.enqueue(queue: it, task: "note") }

    worker = Stampeded::Worker.new(queues: :all, poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { ids.all? { job_row(it)["state"] == "completed" } }

    assert(ids.all? { job_row(it)["state"] == "completed" })
  end

  def test_a_queue_that_appears_later_is_picked_up
    worker = Stampeded::Worker.new(queues: "*", poll_interval: 0.05)
    worker.register("note") { nil }
    late = nil
    run_worker(worker) do
      late ||= Stampeded.enqueue(queue: "late", task: "note")
      job_row(late)["state"] == "completed"
    end

    assert_equal "completed", job_row(late)["state"]
  end

  def test_excluded_queues_are_left_alone
    parked = Stampeded.enqueue(queue: "dlq", task: "note")
    served = Stampeded.enqueue(queue: "work", task: "note")

    worker = Stampeded::Worker.new(queues: :all, except: "dlq", poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(served)["state"] == "completed" }
    sleep 0.2

    assert_equal "available", job_row(parked)["state"]
  end

  def test_the_worker_registers_as_serving_every_queue
    seen = Queue.new
    worker = Stampeded::Worker.new(queues: :all, poll_interval: 0.05)
    worker.register("x") { seen << conn { |c| c.exec("SELECT queues FROM stampeded_workers")[0]["queues"] } }
    Stampeded.enqueue(queue: "anything", task: "x")

    run_worker(worker) { !seen.empty? }

    assert_equal "{*}", seen.pop
  end

  def test_policies_still_apply_to_discovered_queues
    Stampeded::QueuePolicy.set("held")
    conn { |c| c.exec("UPDATE stampeded_queue_policies SET verdict = 'hold', decided_at = now() WHERE queue = 'held'") }
    held = Stampeded.enqueue(queue: "held", task: "note")
    open = Stampeded.enqueue(queue: "open", task: "note")

    worker = Stampeded::Worker.new(queues: :all, poll_interval: 0.05)
    worker.register("note") { nil }
    run_worker(worker) { job_row(open)["state"] == "completed" }
    sleep 0.2

    assert_equal "available", job_row(held)["state"]
  end
end

class AllQueuesValidationTest < Minitest::Test
  def test_a_wildcard_cannot_be_mixed_with_names
    assert_raises(ArgumentError) { Stampeded::Worker.new(queues: "default,*") }
    assert_raises(ArgumentError) { Stampeded::Worker.new(queues: ["default", "*"]) }
  end

  def test_except_needs_every_queue
    assert_raises(ArgumentError) { Stampeded::Worker.new(queues: "default", except: "dlq") }
  end

  def test_all_queues_from_config
    Stampeded.config.queues = :all
    Stampeded.config.except_queues = %w[dlq]

    worker = Stampeded::Worker.new
    assert_predicate worker, :all_queues?
    assert_equal %w[dlq], worker.instance_variable_get(:@except)
    refute_predicate Stampeded::Worker.new(queue: "mail"), :all_queues?, "an explicit queue wins over config"
  ensure
    Stampeded.config.queues = nil
    Stampeded.config.except_queues = nil
  end
end
