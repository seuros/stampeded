# frozen_string_literal: true

require "json"

module Stampeded
  class JobContext
    extend Forwardable
    def_delegators :@job, :id, :queue, :task, :args, :meta, :attempt, :max_attempts,
                   :attempts_remaining, :last_attempt?, :tenant_id, :destination,
                   :checkpoint, :resumed?, :checkpoint_seq, :row

    attr_reader :job, :worker_id

    def initialize(job, worker_id)
      @job = job
      @worker_id = worker_id
    end

    def checkpoint!(state = nil, conn: nil, **rest)
      state ||= rest
      raise ArgumentError, "checkpoint! needs some state" if state.nil? || state.empty?

      sql = "SELECT stampeded_checkpoint($1, $2, $3, $4, $5)"
      params = [job.queue, job.id, worker_id, job.attempt, JSON.generate(state)]

      seq =
        if conn
          conn.exec_params(sql, params)[0]["stampeded_checkpoint"]
        else
          Connection.with { |c| c.exec_params(sql, params)[0]["stampeded_checkpoint"] }
        end

      !seq.nil?
    end

    def steps
      @steps ||= Steps.new(self)
    end

    def step(name, cursor: nil, &) = steps.step(name, cursor: cursor, &)
    def touch!
      ok = Connection.own do |c|
        c.exec_params("SELECT stampeded_touch($1, $2, $3, $4)",
                      [job.queue, job.id, worker_id, job.attempt])[0]["stampeded_touch"]
      end
      Stampeded.truthy?(ok)
    end
  end
end
