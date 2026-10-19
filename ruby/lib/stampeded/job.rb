# frozen_string_literal: true

require "json"
require "time"

module Stampeded
  class Job
    ATTRIBUTES = %w[
      id queue task args meta state priority attempt max_attempts
      interruptions max_interruptions deferrals max_deferrals
      tenant_id destination dead_letter_queue expire_seconds
      scheduled_at last_error failure_kind
    ].freeze

    attr_reader :row

    def initialize(row, decoded: false)
      @row = row
      @decoded = decoded
    end

    def id       = row["id"]
    def queue    = row["queue"]
    def task     = row["task"]
    def state    = row["state"]
    def terminal?  = Lifecycle.terminal?(state)
    def claimable? = Lifecycle.claimable?(state)

    def available_events = Lifecycle.events_from(state)

    def respond_to_missing?(name, include_private = false) = state_predicate?(name) || super

    def method_missing(name, *args)
      return super unless args.empty? && state_predicate?(name)

      state.to_s == name.to_s.delete_suffix("?")
    end
    def attempt  = row["attempt"].to_i
    def max_attempts = row["max_attempts"].to_i
    def interruptions = row["interruptions"].to_i
    def deferrals = row["deferrals"].to_i
    def tenant_id = row["tenant_id"]
    def destination = row["destination"]

    def args
      @args ||= decode(row["args"])
    end

    def meta
      @meta ||= decode(row["meta"])
    end

    def checkpoint
      return @checkpoint if defined?(@checkpoint)

      raw = row["checkpoint"]
      @checkpoint = raw.nil? ? nil : decode(raw)
    end

    def resumed? = !checkpoint.nil?

    def checkpoint_seq = row["checkpoint_seq"].to_i
    def attempts_remaining = [max_attempts - attempt, 0].max

    def last_attempt? = attempts_remaining.zero?

    def to_h = ATTRIBUTES.to_h { |a| [a, row[a]] }

    def inspect
      "#<Stampeded::Job #{id} #{queue}/#{task} attempt=#{attempt}/#{max_attempts}>"
    end

    private

    def state_predicate?(name) = name.end_with?("?") && Lifecycle.states.include?(name.to_s.delete_suffix("?").to_sym)

    def decode(value)
      case value
      when nil then {}
      when String then @decoded ? value : JSON.parse(value)
      else value
      end
    end
  end
end
