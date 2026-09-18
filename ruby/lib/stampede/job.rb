# frozen_string_literal: true

require "json"
require "time"

module Stampede
  # One claimed row.
  #
  # A value object over the SQL function's output. It deliberately has no
  # save/update methods: every state transition goes through a SQL function so
  # that the Ruby worker and the Rust worker cannot diverge (plans/05).
  class Job
    ATTRIBUTES = %w[
      id queue task args state priority attempt max_attempts
      interruptions max_interruptions deferrals max_deferrals
      tenant_id destination dead_letter_queue expire_seconds
      scheduled_at last_error failure_kind
    ].freeze

    attr_reader :row

    def initialize(row)
      @row = row
    end

    def id       = row["id"]
    def queue    = row["queue"]
    def task     = row["task"]
    def state    = row["state"]
    def attempt  = row["attempt"].to_i
    def max_attempts = row["max_attempts"].to_i
    def interruptions = row["interruptions"].to_i
    def deferrals = row["deferrals"].to_i
    def tenant_id = row["tenant_id"]
    def destination = row["destination"]

    def args
      @args ||= JSON.parse(row["args"] || "{}")
    end

    # Resume state saved by a previous attempt, or nil on the first.
    #
    # A job that died at row 50,000 of 100,000 gets its cursor back rather than
    # starting again at row 1. Nobody else in the Postgres cohort has this.
    def checkpoint
      return @checkpoint if defined?(@checkpoint)

      raw = row["checkpoint"]
      @checkpoint = raw.nil? ? nil : JSON.parse(raw)
    end

    def resumed? = !checkpoint.nil?

    def checkpoint_seq = row["checkpoint_seq"].to_i

    # Attempts left before the job is discarded.
    #
    # `attempt` was already incremented at claim, so a job currently executing
    # has ALREADY SPENT the attempt it is using. This is the plans/04 poison
    # brake and the most common thing to get wrong when reading these numbers.
    def attempts_remaining = [max_attempts - attempt, 0].max

    def last_attempt? = attempts_remaining.zero?

    def to_h = ATTRIBUTES.to_h { |a| [a, row[a]] }

    def inspect
      "#<Stampede::Job #{id} #{queue}/#{task} attempt=#{attempt}/#{max_attempts}>"
    end
  end
end
