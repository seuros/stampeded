# frozen_string_literal: true

module Stampede
  # Class-level DSL for the Stampede-specific columns ActiveJob has no concept
  # of, plus the retry_on guard and the resume helpers.
  module ActiveJobExt
    def self.included(base)
      base.extend(ClassMethods)
    end

    # Resume state from a previous attempt, or nil on the first run.
    #
    #   def perform(file)
    #     cursor = stampede_checkpoint&.dig("cursor") || 0
    #     rows[cursor..].each_slice(1_000) do |chunk|
    #       import(chunk)                          # commit FIRST
    #       cursor += chunk.size
    #       stampede_checkpoint!(cursor: cursor)   # then record it
    #     end
    #   end
    def stampede_checkpoint
      Stampede::Current.context&.checkpoint
    end

    # Save resume state. Returns false if this worker no longer owns the job --
    # the reaper reclaimed it, someone else is running it now, and this attempt
    # should stop rather than keep working.
    #
    # COMMIT YOUR CHUNK FIRST. Checkpointing before the work is durable means a
    # crash between the two replays that chunk on resume, which for a job that
    # is not idempotent is silent duplication.
    def stampede_checkpoint!(state = nil, conn: nil, **rest)
      ctx = Stampede::Current.context
      raise Stampede::Error, "no Stampede job context: is this running under a Stampede worker?" if ctx.nil?

      ctx.checkpoint!(state || rest, conn: conn)
    end

    # True when this attempt picked up where a previous one left off.
    def stampede_resumed?
      !Stampede::Current.context&.checkpoint.nil?
    end

    module ClassMethods
      def stampede_options
        @stampede_options ||= superclass.respond_to?(:stampede_options) ? superclass.stampede_options.dup : {}
      end

      # Per-job-class settings that live on the row.
      #
      #   class ChargeCard < ApplicationJob
      #     stampede max_attempts: 5, destination: "stripe.com",
      #              dead_letter_queue: "payments_dlq"
      #   end
      def stampede(**options)
        unknown = options.keys - %i[max_attempts destination dead_letter_queue
                                    expire_seconds unique unique_period]
        raise ArgumentError, "unknown stampede options: #{unknown.join(", ")}" if unknown.any?

        stampede_options.merge!(options)
      end

      # The guard.
      #
      # ActiveJob's retry_on re-enqueues the job as a NEW ROW; Stampede counts
      # attempts at claim. Together they are two independent budgets, and every
      # row reports attempt 1, so the dashboard and the metrics are wrong in a
      # way nobody would think to check. See Stampede::RetryConflict.
      def retry_on(*, **)
        raise Stampede::RetryConflict, name || "This job" if stampede_adapter?

        super
      end

      def stampede_adapter?
        adapter = respond_to?(:queue_adapter) ? queue_adapter : nil
        adapter.class.name.to_s.include?("Stampede")
      rescue StandardError
        false
      end
    end
  end
end
