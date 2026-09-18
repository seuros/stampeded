# frozen_string_literal: true

module Stampede
  # The job being executed on this thread.
  #
  # ActiveJob::Base.execute takes a payload and returns nothing useful -- there
  # is no seam to hand a context through, and wrapping `perform` would mean
  # re-implementing ActiveJob's callback and instrumentation handling. So the
  # worker parks the context here for the duration of the call and the job
  # reads it back. GoodJob::CurrentThread is the same pattern for the same
  # reason.
  #
  # THREAD-local, matching the worker's thread pool. A job that spawns its own
  # threads and checkpoints from them will find nothing, which is correct:
  # those threads do not own the job.
  module Current
    KEY = :__stampede_job_context

    module_function

    def context = Thread.current[KEY]

    def with(context)
      previous = Thread.current[KEY]
      Thread.current[KEY] = context
      yield
    ensure
      Thread.current[KEY] = previous
    end
  end
end
