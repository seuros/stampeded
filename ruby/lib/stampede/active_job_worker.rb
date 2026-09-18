# frozen_string_literal: true

require "stampede"

module Stampede
  # A worker that executes ActiveJob jobs.
  #
  # This is the half the Rust daemon cannot do (plans/16 §2): an ActiveJob job
  # is a Ruby class, and executing one means having Rails loaded. Everything
  # else -- scheduling, reaping, pruning, expiring, the dashboard -- stays in
  # the daemon, because those operate on rows.
  class ActiveJobWorker < Worker
    def initialize(**kwargs)
      super
      # One handler for every task: ActiveJob's own dispatch already knows how
      # to turn a serialised payload back into a job and run it, including
      # callbacks, instrumentation and discard_on.
      @handlers.default_proc = proc { |_h, _task| method(:execute_active_job) }
    end

    private

    def execute_active_job(job)
      payload = job.args

      # A recurring job arrives with no job_id: Stampede::Schedule stores the
      # payload without one so that every occurrence does not share a single
      # id. Fill it from the row, which also makes the ActiveJob id and the
      # Stampede id the same value -- a job seen in a Rails log can be looked
      # up directly in the dashboard.
      payload = payload.merge("job_id" => job.id) if payload["job_id"].nil?
      payload = payload.merge("enqueued_at" => Time.now.utc.iso8601) if payload["enqueued_at"].nil?

      # ActiveJob::Base.execute is the same entry point every other adapter
      # uses. It resolves the class, runs callbacks, and raises out of
      # `rescue_from`/`discard_on` handling -- so an exception reaching us is
      # genuinely unhandled and should be recorded as a failure.
      ActiveJob::Base.execute(payload)
    rescue NameError => e
      # A job class that no longer exists: a rename deployed to the enqueuer
      # but not to the workers. Unrecoverable rather than retryable, because
      # no amount of waiting will bring the constant back.
      raise UnknownTask, "#{job.task}: #{e.message}"
    end
  end
end
