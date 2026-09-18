# frozen_string_literal: true

require "test_helper"

CRON_RAN = Queue.new

class NightlyJob < ActiveJob::Base
  def perform(*args) = CRON_RAN << args
end

class ScheduleTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    CRON_RAN.clear
  end

  def test_the_queue_is_a_name_not_a_stringified_lambda
    # ActiveJob's queue_name class attribute holds a LAMBDA by default. Reading
    # it off the class gives "#<Proc:0x...>", which is a valid queue name that
    # no worker polls -- so the schedule fires forever into a queue nobody
    # reads, silently.
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *")

    queue = conn { |c| c.exec("SELECT queue FROM stampeded_schedules")[0]["queue"] }
    refute_match(/Proc|lambda|0x/, queue, "queue name is a stringified lambda: #{queue}")
    assert_equal "default", queue
  end

  def test_a_recurring_job_stores_a_payload_the_worker_can_deserialise
    # The bug this exists for: a schedule's args are copied VERBATIM onto every
    # firing, and the daemon has no idea what ActiveJob is. Storing plain args
    # made both occurrences discard as unrecoverable.
    Stampeded::Schedule.recurring(key: "nightly", job: NightlyJob, cron: "0 3 * * *", args: [42])

    row = conn { |c| c.exec("SELECT * FROM stampeded_schedules WHERE key='nightly'")[0] }
    payload = JSON.parse(row["args"])

    assert_equal "NightlyJob", payload["job_class"]
    assert_equal [42], payload["arguments"]
    assert_equal "0 3 * * *", row["cron_expr"]
    assert_nil row["interval_secs"], "cron and interval are mutually exclusive"
  end

  def test_the_payload_carries_no_job_id
    # Every occurrence would otherwise share one ActiveJob id, which makes logs
    # and any tracing built on job_id useless.
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *")

    payload = conn { |c| JSON.parse(c.exec("SELECT args FROM stampeded_schedules")[0]["args"]) }
    assert_nil payload["job_id"]
  end

  def test_a_fired_occurrence_is_executed_by_the_worker
    # The end-to-end property: the scheduler (Rust, in production) enqueues from
    # the schedule row, and the Ruby worker must be able to run what comes out.
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *", args: ["cron"])
    conn { |c| c.exec("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'") }

    # Fire it the way the daemon does.
    fired = conn do |c|
      rows = c.exec("SELECT * FROM stampeded_claim_due_schedules(10)")
      rows.map { |r| c.exec_params("SELECT stampeded_fire_schedule($1, now() + interval '1 hour')", [r["key"]]) }
      rows.ntuples
    end
    assert_equal 1, fired

    worker = Stampeded::ActiveJobWorker.new(queue: "default", poll_interval: 0.05)
    t = Thread.new { worker.run }
    deadline = Time.now + 10
    sleep 0.05 until !CRON_RAN.empty? || Time.now > deadline
    worker.stop
    t.join(5)

    refute_empty CRON_RAN, "the occurrence was never executed"
    assert_equal ["cron"], CRON_RAN.pop
  ensure
    t&.kill
  end

  def test_the_job_id_is_filled_from_the_row_so_it_is_unique_per_occurrence
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *")
    conn { |c| c.exec("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'") }
    conn do |c|
      c.exec("SELECT * FROM stampeded_claim_due_schedules(10)").each do |r|
        c.exec_params("SELECT stampeded_fire_schedule($1, now() + interval '1 hour')", [r["key"]])
      end
    end

    row = conn { |c| c.exec("SELECT id, args FROM stampeded_jobs")[0] }
    assert_nil JSON.parse(row["args"])["job_id"], "stored payload has none"

    # The worker fills it in, which also ties the ActiveJob id to the row id.
    job = Stampeded::Job.new(row)
    assert_equal row["id"], job.id
  end

  def test_pause_and_remove
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *")
    Stampeded::Schedule.pause("n")
    assert_equal "t", conn { |c| c.exec("SELECT paused FROM stampeded_schedules")[0]["paused"] }

    Stampeded::Schedule.remove("n")
    assert_empty Stampeded::Schedule.all
  end
end

class ScheduleValidationTest < Minitest::Test
  include StampededTest::Isolation

  def test_an_unparseable_cron_expression_is_refused_at_write_time
    # The Rust side validated on write; Ruby did not. So `cron: "garbage"` was
    # stored happily and then failed at FIRE time -- where the scheduler logs a
    # warning, leaves the row due, and repeats that every tick FOREVER. A
    # schedule that never runs plus a log line every five seconds, and nothing
    # pointing at the typo that caused it.
    error = assert_raises(Stampeded::Error) do
      Stampeded::Schedule.recurring(key: "bad", job: NightlyJob, cron: "garbage")
    end
    assert_match(/Five fields are required/, error.message)

    assert_raises(Stampeded::Error) do
      Stampeded::Schedule.recurring(key: "bad", job: NightlyJob, cron: "0 3 * *")
    end

    assert_empty Stampeded::Schedule.all, "nothing may be created"
  end

  def test_real_expressions_still_work
    # A shape check that rejected valid cron would be worse than none.
    ["0 3 * * *", "*/15 * * * *", "0 9-17 * * mon-fri", "0 0 1,15 * *", "* * * * *"].each do |expr|
      Stampeded::Schedule.remove("ok")
      Stampeded::Schedule.recurring(key: "ok", job: NightlyJob, cron: expr)
    end
  end

  def test_a_schedule_needs_a_key
    assert_raises(Stampeded::Error) do
      Stampeded::Schedule.recurring(key: "", job: NightlyJob, cron: "* * * * *")
    end
    assert_raises(Stampeded::Error) do
      Stampeded::Schedule.recurring(key: "  ", job: NightlyJob, cron: "* * * * *")
    end
  end
end
