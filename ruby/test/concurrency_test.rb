# frozen_string_literal: true

require "test_helper"

RAN = Queue.new

class ConcurrentJob < ActiveJob::Base
  def perform(n) = RAN << n
end

class ConcurrencyTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    RAN.clear
  end

  def test_several_workers_run_every_job_exactly_once
    30.times { |i| ConcurrentJob.perform_later(i) }

    workers = 4.times.map do
      Stampeded::ActiveJobWorker.new(queue: "default", concurrency: 2, poll_interval: 0.05)
    end
    threads = workers.map { |w| Thread.new { w.run } }

    deadline = Time.now + 20
    sleep 0.05 until RAN.size >= 30 || Time.now > deadline
    workers.each(&:stop)
    threads.each { |t| t.join(5) }

    ran = []
    ran << RAN.pop until RAN.empty?

    assert_equal 30, ran.size, "every job must run"
    assert_equal ran.uniq.sort, (0...30).to_a, "each exactly once, none twice"

    states = conn do |c|
      c.exec("SELECT state, count(*) FROM stampeded_jobs GROUP BY state")
       .each_with_object({}) { |r, h| h[r["state"]] = r["count"].to_i }
    end
    assert_equal({ "completed" => 30 }, states)
  ensure
    threads&.each(&:kill)
  end

  def test_the_counters_agree_with_reality_after_concurrent_work
    20.times { |i| ConcurrentJob.perform_later(i) }

    workers = 3.times.map do
      Stampeded::ActiveJobWorker.new(queue: "default", concurrency: 3, poll_interval: 0.05)
    end
    threads = workers.map { |w| Thread.new { w.run } }

    deadline = Time.now + 20
    sleep 0.05 until RAN.size >= 20 || Time.now > deadline
    workers.each(&:stop)
    threads.each { |t| t.join(5) }

    assert_equal({ "completed" => 20 }, Stampeded.depth("default"))

    actual = conn { |c| c.exec("SELECT count(*) FROM stampeded_jobs WHERE state='completed'")[0]["count"].to_i }
    assert_equal 20, actual, "the counter and the table must agree"
  ensure
    threads&.each(&:kill)
  end

  def test_a_thundering_herd_of_identical_jobs_enqueues_one
    results = Queue.new
    threads = 40.times.map do
      Thread.new do
        results << Stampeded.enqueue(queue: "default", task: "refresh", unique: "cache:products")
      end
    end
    threads.each(&:join)

    ids = []
    ids << results.pop until results.empty?
    assert_equal 1, ids.compact.size, "exactly one enqueue may win"
    assert_equal 1, conn { |c| c.exec("SELECT count(*) FROM stampeded_jobs")[0]["count"].to_i }
  end
end
