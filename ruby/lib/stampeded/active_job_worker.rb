# frozen_string_literal: true

require "stampeded"

module Stampeded
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

      # A recurring job arrives with no job_id: Stampeded::Schedule stores the
      # payload without one so that every occurrence does not share a single
      # id. Fill it from the row, which also makes the ActiveJob id and the
      # Stampeded id the same value -- a job seen in a Rails log can be looked
      # up directly in the dashboard.
      payload = payload.merge("job_id" => job.id) if payload["job_id"].nil?
      payload = payload.merge("enqueued_at" => Time.now.utc.iso8601) if payload["enqueued_at"].nil?

      # ActiveJob::Base.execute is the same entry point every other adapter
      # uses. It resolves the class, runs callbacks, and raises out of
      # `rescue_from`/`discard_on` handling -- so an exception reaching us is
      # genuinely unhandled and should be recorded as a failure.
      # Resolve the class FIRST, so that "this job class is gone" is
      # distinguishable from "this job raised a NameError".
      #
      # This used to rescue NameError around the whole execution -- and
      # NoMethodError IS A NameError, so any `nil.foo` typo inside any job was
      # misclassified as an unknown task, marked unrecoverable and DISCARDED
      # without a single retry. A transient nil during a deploy would silently
      # destroy jobs. Found when a typo in a test job produced
      # "undefined method 'cursor'" reported as a missing task.
      klass = payload["job_class"]
      begin
        Object.const_get(klass)
      rescue NameError
        # A rename deployed to the enqueuer but not to the workers. No amount
        # of waiting brings the constant back, so this one really is terminal.
        raise UnknownTask, "#{klass} is not defined in this worker"
      end

      # Parked for the duration so the job can reach it via the helpers in
      # ActiveJobExt. ActiveJob::Base.execute gives us no other seam.
      Current.with(job) { ActiveJob::Base.execute(payload) }
    end
  end
end
