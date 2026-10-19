# frozen_string_literal: true

module Stampeded
  module ContinuableExt
    PROGRESS_EVERY = 1.0

    def checkpoint!
      stampeded_save_progress(force: queue_adapter.stopping?) if Current.context
      super
    end

    private

    def resume_job(exception)
      return super unless Current.context

      stampeded_save_progress(force: true)
      raise exception unless exception.is_a?(ActiveJob::Continuation::Interrupt)

      if max_resumptions && resumptions >= max_resumptions
        raise ActiveJob::Continuation::ResumeLimitError, "Job was resumed a maximum of #{max_resumptions} times"
      end

      raise Requeue, exception.message
    end

    def stampeded_save_progress(force:)
      progress = continuation.to_h
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      fresh = progress["completed"] == @stampeded_saved_steps && now - (@stampeded_saved_at || -PROGRESS_EVERY) < PROGRESS_EVERY
      return if fresh && !force

      Current.context.checkpoint!("activejob" => { "continuation" => progress, "resumptions" => resumptions })
      @stampeded_saved_steps = progress["completed"]
      @stampeded_saved_at = now
    end
  end
end

ActiveSupport.on_load(:active_job_continuable) { prepend Stampeded::ContinuableExt }
