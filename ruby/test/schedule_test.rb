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
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *")

    queue = conn { |c| c.exec("SELECT queue FROM stampeded_schedules")[0]["queue"] }
    refute_match(/Proc|lambda|0x/, queue, "queue name is a stringified lambda: #{queue}")
    assert_equal "default", queue
  end

  def test_a_recurring_job_stores_a_payload_the_worker_can_deserialise
    Stampeded::Schedule.recurring(key: "nightly", job: NightlyJob, cron: "0 3 * * *", args: [42])

    row = conn { |c| c.exec("SELECT * FROM stampeded_schedules WHERE key='nightly'")[0] }
    activejob = JSON.parse(row["meta"])["activejob"]

    assert_equal({}, JSON.parse(row["args"]))
    assert_equal "NightlyJob", activejob["job_class"]
    assert_equal [42], activejob["arguments"]
    assert_equal "0 3 * * *", row["cron_expr"]
    assert_nil row["interval_secs"], "cron and interval are mutually exclusive"
  end

  def test_the_payload_carries_no_job_id
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *")

    meta = conn { |c| JSON.parse(c.exec("SELECT meta FROM stampeded_schedules")[0]["meta"]) }
    assert_equal "NightlyJob", meta.dig("activejob", "job_class")
    assert_nil meta.dig("activejob", "job_id")
  end

  def test_a_fired_occurrence_is_executed_by_the_worker
    Stampeded::Schedule.recurring(key: "n", job: NightlyJob, cron: "* * * * *", args: ["cron"])
    conn { |c| c.exec("UPDATE stampeded_schedules SET next_run_at = now() - interval '1 minute'") }

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
    ["0 3 * * *", "*/15 * * * *", "0 9-17 * * mon-fri", "0 0 1,15 * *", "* * * * *"].each do |expr|
      Stampeded::Schedule.remove("ok")
      Stampeded::Schedule.recurring(key: "ok", job: NightlyJob, cron: expr)
    end
  end

  def test_what_the_daemon_cannot_parse_is_refused
    ["61 * * * *", "0 25 * * *", "0 3 * * 9"].each do |expr|
      assert_raises(Stampeded::Error, expr) do
        Stampeded::Schedule.recurring(key: "bad", job: NightlyJob, cron: expr)
      end
    end
    error = assert_raises(Stampeded::Error) do
      Stampeded::Schedule.recurring(key: "bad", job: NightlyJob, cron: "0 3 * * *", timezone: "Mars/Olympus")
    end
    assert_match(/Mars\/Olympus/, error.message)
    refute_match(/Five fields/, error.message)
    assert_raises(Stampeded::Error) do
      Stampeded::Schedule.recurring(key: "bad", job: NightlyJob, cron: "61 * * * *", dst_policy: "instant")
    end
    assert_empty Stampeded::Schedule.all
  end

  def test_the_dst_policy_follows_the_expression_unless_given
    policy = lambda do |key, **opts|
      Stampeded::Schedule.recurring(key: key, job: NightlyJob, **opts)
      conn { |c| c.exec_params("SELECT dst_policy FROM stampeded_schedules WHERE key = $1", [key])[0]["dst_policy"] }
    end
    assert_equal "wall_clock", policy.("nightly", cron: "30 1 * * *", timezone: "Africa/Casablanca")
    assert_equal "instant", policy.("often", cron: "*/15 * * * *")
    assert_equal "instant", policy.("chosen", cron: "30 1 * * *", dst_policy: "instant")
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
