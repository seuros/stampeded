# frozen_string_literal: true

require "test_helper"

PROCESSED = Queue.new

class ResumableImportJob < ActiveJob::Base
  TOTAL = 10

  def perform
    cursor = stampeded_checkpoint&.dig("cursor") || 0
    PROCESSED << [:started_at, cursor]

    (cursor...TOTAL).each do |i|
      PROCESSED << [:did, i]
      stampeded_checkpoint!(cursor: i + 1)

      raise "worker died at #{i}" if i == 4 && !stampeded_resumed?
    end
  end
end

class ResumeTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    PROCESSED.clear
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

  def test_the_second_attempt_continues_instead_of_starting_over
    job = ResumableImportJob.perform_later
    id = job.provider_job_id

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    drain(worker) { job_row(id)["state"] == "completed" }

    events = []
    events << PROCESSED.pop until PROCESSED.empty?

    starts = events.select { |e| e.first == :started_at }.map(&:last)
    assert_equal [0, 5], starts, "the second attempt must start where the first stopped"

    did = events.select { |e| e.first == :did }.map(&:last)
    assert_equal (0...ResumableImportJob::TOTAL).to_a, did
    assert_equal did.uniq, did, "no unit of work may be repeated"
  end

  def test_a_job_can_tell_whether_it_resumed
    job = ResumableImportJob.perform_later
    drain(Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)) do
      job_row(job.provider_job_id)["state"] == "completed"
    end

    starts = []
    starts << PROCESSED.pop until PROCESSED.empty?
    assert_includes starts.map(&:last), 0
  end

  def test_checkpointing_outside_a_worker_is_a_clear_error
    error = assert_raises(Stampeded::Error) do
      ResumableImportJob.new.stampeded_checkpoint!(cursor: 1)
    end
    assert_match(/Stampeded worker/, error.message)
  end

  def test_reading_a_checkpoint_outside_a_worker_is_just_nil
    assert_nil ResumableImportJob.new.stampeded_checkpoint
  end

  def test_a_plain_worker_gets_a_context_too
    seen = Queue.new
    id = Stampeded.enqueue(queue: "default", task: "chunky")

    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("chunky") do |ctx|
      seen << ctx.checkpoint
      ctx.checkpoint!(cursor: 42)
    end

    drain(worker) { job_row(id)["state"] == "completed" }

    assert_nil seen.pop, "first attempt has nothing to resume from"
    assert_operator job_row(id)["checkpoint_seq"].to_i, :>=, 1
  end
end

class ResumeTransactionTest < Minitest::Test
  include StampededTest::Isolation

  def test_a_checkpoint_rolls_back_with_the_work_it_describes
    q = Stampeded
    id = q.enqueue(queue: "default", task: "chunky")
    worker_id = conn do |c|
      c.exec_params("INSERT INTO stampeded_workers (hostname,pid,queues) VALUES ($1,$2,$3) RETURNING id",
                    ["h", 1, "{default}"])[0]["id"]
    end

    job = conn { |c| Stampeded::Job.new(c.exec_params("SELECT * FROM stampeded_claim($1,$2,$3)",
                                                     ["default", worker_id, 1])[0]) }
    ctx = Stampeded::JobContext.new(job, worker_id)

    Stampeded::Connection.own do |c|
      c.exec("BEGIN")
      ctx.checkpoint!({ cursor: 500 }, conn: c)
      c.exec("ROLLBACK")
    end

    row = job_row(id)
    assert_nil row["checkpoint"],
               "a rolled-back chunk must not leave a cursor claiming it was done"
    assert_equal 0, row["checkpoint_seq"].to_i
  end

  def test_a_committed_checkpoint_survives
    id = Stampeded.enqueue(queue: "default", task: "chunky")
    worker_id = conn do |c|
      c.exec_params("INSERT INTO stampeded_workers (hostname,pid,queues) VALUES ($1,$2,$3) RETURNING id",
                    ["h", 1, "{default}"])[0]["id"]
    end
    job = conn { |c| Stampeded::Job.new(c.exec_params("SELECT * FROM stampeded_claim($1,$2,$3)",
                                                    ["default", worker_id, 1])[0]) }
    ctx = Stampeded::JobContext.new(job, worker_id)

    Stampeded::Connection.own do |c|
      c.exec("BEGIN")
      ctx.checkpoint!({ cursor: 500 }, conn: c)
      c.exec("COMMIT")
    end

    assert_equal({ "cursor" => 500 }, JSON.parse(job_row(id)["checkpoint"]))
  end
end
