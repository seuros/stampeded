# frozen_string_literal: true

module Stampede
  Error = Class.new(StandardError)

  # Raised at class-load when a job declares `retry_on`.
  #
  # Not a warning, deliberately. ActiveJob's retry_on re-enqueues the job as a
  # NEW ROW, while Stampede counts attempts at claim -- so both together give
  # two independent budgets and every attempt column reads 1. A warning gets
  # filtered out of production logs; a raise is seen once, by the person who
  # wrote the line.
  class RetryConflict < Error
    def initialize(job_class)
      super(<<~MSG)
        #{job_class} declares `retry_on`, which conflicts with Stampede's retry accounting.

        ActiveJob's retry_on re-enqueues the job as a NEW job. Stampede counts
        attempts at claim time (that is what stops a job which kills its worker
        from retrying forever). With both active, `retry_on attempts: 5` inside a
        job with max_attempts: 25 can run 125 times, and every row will report
        attempt 1 -- so the dashboard, the metrics and failure_summary all lie.

        Let Stampede own retries instead:

          class #{job_class} < ApplicationJob
            stampede max_attempts: 5, backoff_base_ms: 1_000, backoff_max_ms: 300_000
          end

        `discard_on` is fine and needs no change -- a discarded exception never
        reaches us, so the job simply completes.
      MSG
    end
  end

  # The daemon binary is not installed for this platform.
  class DaemonNotFound < Error; end

  # A job was claimed for a task with no registered handler.
  class UnknownTask < Error; end
end
