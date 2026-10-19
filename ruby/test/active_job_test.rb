# frozen_string_literal: true

require "test_helper"

class GreetJob < ActiveJob::Base
  cattr_accessor :ran, default: [] if respond_to?(:cattr_accessor)
  @@ran = []

  def self.ran = @@ran

  def perform(who)
    @@ran << who
  end
end

class ChargeJob < ActiveJob::Base
  stampeded max_attempts: 5, destination: "stripe.com", dead_letter_queue: "payments_dlq"

  def perform(*); end
end

class BoomJob < ActiveJob::Base
  def perform = raise("boom")
end

class ActiveJobTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    ActiveJob::Base.queue_adapter = ActiveJob::QueueAdapters::StampededAdapter.new
    GreetJob.ran.clear
  end

  def test_perform_later_writes_a_row_and_sets_provider_job_id
    job = GreetJob.perform_later("ruby")

    refute_nil job.provider_job_id
    row = job_row(job.provider_job_id)
    assert_equal "GreetJob", row["task"], "the task is the job class, so a rename is visible"
    assert_equal "available", row["state"]
  end

  def test_perform_later_at_schedules_rather_than_enqueues_now
    job = GreetJob.set(wait: 1.hour).perform_later("later")
    row = job_row(job.provider_job_id)

    assert_operator Time.parse(row["scheduled_at"]), :>, Time.now + 3000
  end

  def test_the_class_level_dsl_reaches_the_row
    job = ChargeJob.perform_later
    row = job_row(job.provider_job_id)

    assert_equal "stripe.com", row["destination"]
    assert_equal "payments_dlq", row["dead_letter_queue"]
    assert_equal "5", row["max_attempts"]
  end

  def test_retry_on_is_refused_at_class_definition
    error = assert_raises(Stampeded::RetryConflict) do
      Class.new(ActiveJob::Base) do
        def self.name = "RetryingJob"
        retry_on StandardError, attempts: 5
      end
    end

    assert_match(/Stampeded owns retries/, error.message)
    assert_match(/max_attempts/, error.message, "must name the Stampeded equivalent")
  end

  def test_retry_on_inside_an_installed_gem_is_let_through
    gem_file = File.join(Gem.path.first, "gems", "vendored-1.0", "app", "jobs", "vendored_job.rb")
    klass = Class.new(ActiveJob::Base) { def self.name = "VendoredJob" }

    _, err = capture_io { klass.class_eval("retry_on StandardError, attempts: 5", gem_file, 1) }

    assert_match(/VendoredJob .* declares retry_on/, err)
    assert(klass.rescue_handlers.any? { |(exception, _)| exception == "StandardError" })
  end

  def test_discard_on_is_left_alone
    klass = Class.new(ActiveJob::Base) do
      def self.name = "DiscardingJob"
      discard_on ArgumentError
    end

    assert klass
  end

  def test_enqueue_all_reports_how_many_were_created
    adapter = ActiveJob::Base.queue_adapter
    jobs = 3.times.map { GreetJob.new("bulk") }

    assert_equal 3, adapter.enqueue_all(jobs)
    assert_equal 3, Stampeded.depth("default")["available"]
  end

  def test_a_rolled_back_transaction_takes_its_jobs_with_it
    conn = PG.connect(Stampeded.config.database_url)
    conn.exec("BEGIN")
    conn.exec_params(
      "INSERT INTO stampeded_jobs (queue, task) VALUES ($1, $2)", ["default", "GreetJob"]
    )
    assert_equal 1, conn.exec("SELECT count(*) FROM stampeded_jobs")[0]["count"].to_i
    conn.exec("ROLLBACK")

    assert_equal 0, conn.exec("SELECT count(*) FROM stampeded_jobs")[0]["count"].to_i,
                 "the job must vanish with the transaction that created it"
    conn.close
  end
end
