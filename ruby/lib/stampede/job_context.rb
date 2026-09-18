# frozen_string_literal: true

require "json"

module Stampede
  # What a handler gets: the job, plus the ability to save resume state.
  #
  # Delegates to the job, so `ctx.args` and `ctx.id` read as before. The
  # addition is #checkpoint -- saving progress needs this worker's id, and a
  # handler has no other way to get it.
  class JobContext
    extend Forwardable
    def_delegators :@job, :id, :queue, :task, :args, :attempt, :max_attempts,
                   :attempts_remaining, :last_attempt?, :tenant_id, :destination,
                   :checkpoint, :resumed?, :checkpoint_seq, :row

    attr_reader :job, :worker_id

    def initialize(job, worker_id)
      @job = job
      @worker_id = worker_id
    end

    # Save resume state. Returns false if this worker no longer owns the job,
    # which means the reaper reclaimed it and the work should stop.
    #
    # Checkpointing also counts as a heartbeat, so a job that saves progress
    # regularly cannot be expired by the wall-clock timeout while it is plainly
    # working. And it resets the interruption budget: a job that demonstrably
    # made progress is not the kind of job that kills workers.
    #
    # ORDER MATTERS AND NOTHING CAN ENFORCE IT: commit the chunk, THEN
    # checkpoint. Checkpointing first means a crash between the two replays that
    # chunk on resume -- silent duplication for a job that is not idempotent.
    # This is the one way to use the feature and get wrong answers.
    def checkpoint!(state)
      seq = Connection.own do |c|
        c.exec_params(
          "SELECT stampede_checkpoint($1, $2, $3, $4)",
          [job.queue, job.id, worker_id, JSON.generate(state)]
        )[0]["stampede_checkpoint"]
      end
      !seq.nil?
    end

    # Tell the database the job is progressing, without saving state.
    def touch!
      ok = Connection.own do |c|
        c.exec_params("SELECT stampede_touch($1, $2, $3)",
                      [job.queue, job.id, worker_id])[0]["stampede_touch"]
      end
      ok == "t"
    end
  end
end
