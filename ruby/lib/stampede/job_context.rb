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
    # UNDER RAILS THIS IS ATOMIC WITH YOUR WORK, because it runs on
    # ActiveRecord's connection:
    #
    #   ActiveRecord::Base.transaction do
    #     import(chunk)                        # the work
    #     stampede_checkpoint!(cursor: n)      # and the cursor, together
    #   end
    #
    # Roll back and the cursor rolls back with it, so it can never claim work
    # that was undone. That closes the one hazard this feature otherwise has:
    # as two separate writes, a crash between them replays the chunk on resume,
    # which for a job that is not idempotent is silent duplication.
    #
    # OUTSIDE RAILS, PASS THE CONNECTION. There is no ambient "caller's
    # connection" to join -- Connection.with opens a fresh one per call -- so
    # atomicity has to be explicit:
    #
    #   conn.exec("BEGIN")
    #   import(chunk, conn)
    #   ctx.checkpoint!({ cursor: n }, conn: conn)
    #   conn.exec("COMMIT")
    #
    # River reached the same conclusion and takes the transaction explicitly
    # (ResumableSetStepCursorTx). An earlier version of this method always
    # opened its own connection and so could not be made atomic at all.
    #
    # Checkpointing also counts as a heartbeat, so a job saving progress cannot
    # be expired by the wall-clock timeout while it is plainly working. And it
    # resets the interruption budget: a job that demonstrably made progress is
    # not the kind of job that kills workers.
    # Both call styles work, because the ergonomic one is the common one:
    #
    #   ctx.checkpoint!(cursor: 500)                  # keywords
    #   ctx.checkpoint!({ cursor: 500 }, conn: conn)  # explicit hash + connection
    #   ctx.checkpoint!(cursor: 500, conn: conn)      # or both at once
    def checkpoint!(state = nil, conn: nil, **rest)
      state ||= rest
      raise ArgumentError, "checkpoint! needs some state" if state.nil? || state.empty?

      sql = "SELECT stampede_checkpoint($1, $2, $3, $4)"
      params = [job.queue, job.id, worker_id, JSON.generate(state)]

      seq =
        if conn
          conn.exec_params(sql, params)[0]["stampede_checkpoint"]
        else
          Connection.with { |c| c.exec_params(sql, params)[0]["stampede_checkpoint"] }
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
