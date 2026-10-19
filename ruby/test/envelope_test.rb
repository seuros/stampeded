# frozen_string_literal: true

require "test_helper"

ENVELOPE_RUNS = Queue.new

class KeywordJob < ActiveJob::Base
  def perform(document_id:, pages: 1) = ENVELOPE_RUNS << [:keyword, document_id, pages, job_id]
end

class PositionalJob < ActiveJob::Base
  def perform(id, label) = ENVELOPE_RUNS << [:positional, id, label]
end

class NoArgumentsJob < ActiveJob::Base
  def perform = ENVELOPE_RUNS << [:none]
end

class TimedJob < ActiveJob::Base
  def perform(at:) = ENVELOPE_RUNS << [:timed, at.class]
end

class PortableJob < ActiveJob::Base
  stampeded task: "invoice.parse"

  def perform(document_id:) = ENVELOPE_RUNS << [:portable, document_id]
end

class EnvelopeTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    ENVELOPE_RUNS.clear
  end

  def row(job) = job_row(job.provider_job_id)
  def args(job) = JSON.parse(row(job)["args"])
  def activejob(job) = JSON.parse(row(job)["meta"])["activejob"]

  def run_until(count, timeout: 10)
    worker = Stampeded::ActiveJobWorker.new(queues: :all, poll_interval: 0.05)
    thread = Thread.new { worker.run }
    deadline = Time.now + timeout
    sleep 0.05 until ENVELOPE_RUNS.size >= count || Time.now > deadline
    worker.stop
    thread.join(5)
    Array.new(ENVELOPE_RUNS.size) { ENVELOPE_RUNS.pop }
  ensure
    thread&.kill
  end

  def test_keyword_arguments_are_the_standard_form
    job = KeywordJob.perform_later(document_id: "d-1", pages: 3)

    assert_equal "KeywordJob", row(job)["task"]
    assert_equal({ "document_id" => "d-1", "pages" => 3 }, args(job))
    assert_equal "KeywordJob", activejob(job)["job_class"]
    assert_equal job.job_id, activejob(job)["job_id"]
    refute activejob(job).key?("arguments"), "the standard form needs no second copy"
  end

  def test_no_arguments_is_the_empty_object
    job = NoArgumentsJob.perform_later

    assert_equal({}, args(job))
    refute activejob(job).key?("arguments")
  end

  def test_what_has_no_standard_form_stays_in_activejobs_own
    positional = PositionalJob.perform_later(7, "seven")
    timed = TimedJob.perform_later(at: Time.utc(2026, 10, 19))

    assert_equal({}, args(positional))
    assert_equal [7, "seven"], activejob(positional)["arguments"]
    assert_equal({}, args(timed))
    assert activejob(timed)["arguments"], "a Time has no plain JSON form"
  end

  def test_a_portable_task_is_named_and_its_arguments_are_its_contract
    job = PortableJob.perform_later(document_id: "d-9")

    assert_equal "invoice.parse", row(job)["task"]
    assert_equal({ "document_id" => "d-9" }, args(job))

    error = assert_raises(Stampeded::PortableArgumentsError) { PortableJob.perform_later("d-9") }
    assert_match(/plain JSON values/, error.message)
    assert_raises(Stampeded::PortableArgumentsError) { PortableJob.perform_later(document_id: Time.now) }
  end

  def test_the_worker_runs_every_form
    keyword = KeywordJob.perform_later(document_id: "d-1", pages: 2)
    PositionalJob.perform_later(7, "seven")
    NoArgumentsJob.perform_later
    TimedJob.perform_later(at: Time.utc(2026, 10, 19))

    runs = run_until(4)

    assert_includes runs, [:keyword, "d-1", 2, keyword.job_id], "keywords, and the job_id the caller saw"
    assert_includes runs, [:positional, 7, "seven"]
    assert_includes runs, [:none]
    assert_includes runs, [:timed, Time]
  end

  def test_a_job_enqueued_by_task_name_alone_runs_its_ruby_class
    id = Stampeded.enqueue(queue: "default", task: "invoice.parse", args: { document_id: "from-rust" })

    assert_equal [[:portable, "from-rust"]], run_until(1)
    assert_equal "completed", job_row(id)["state"]
  end

  def test_a_task_name_belongs_to_one_class
    error = assert_raises(ArgumentError) do
      Class.new(ActiveJob::Base) do
        def self.name = "ImpostorJob"
        stampeded task: "invoice.parse"
      end
    end
    assert_match(/already PortableJob's/, error.message)
  end

  def test_a_task_name_is_not_inherited
    subclass = Class.new(PortableJob) { def self.name = "SpecialisedPortableJob" }

    assert_equal "invoice.parse", PortableJob.stampeded_task
    assert_nil subclass.stampeded_task
  end

  def test_a_schedule_stores_keyword_arguments_in_the_standard_form
    Stampeded::Schedule.recurring(key: "parse", job: PortableJob, cron: "0 * * * *", args: { document_id: "d-5" })

    schedule = conn { |c| c.exec("SELECT task, args, meta FROM stampeded_schedules WHERE key = 'parse'")[0] }
    assert_equal "invoice.parse", schedule["task"]
    assert_equal({ "document_id" => "d-5" }, JSON.parse(schedule["args"]))
    assert_equal "PortableJob", JSON.parse(schedule["meta"]).dig("activejob", "job_class")

    assert_raises(Stampeded::PortableArgumentsError) do
      Stampeded::Schedule.recurring(key: "bad", job: PortableJob, cron: "0 * * * *", args: [1])
    end
  end
end
