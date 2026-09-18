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

    # Run a step once, ever -- skipped if an earlier attempt completed it.
    #
    #   def perform(order)
    #     stampede_step("charge") { Stripe.charge(order.total) }   # never twice
    #     path = stampede_step("render") { render_pdf(order) }     # value kept
    #
    #     stampede_step("email", cursor: 0) do |step|
    #       recipients[step.cursor..].each do |to|
    #         Mailer.receipt(to).deliver_now
    #         step.advance!(step.cursor + 1)
    #       end
    #     end
    #   end
    def stampede_step(name, cursor: nil, &)
      ctx = Stampede::Current.context
      raise Stampede::Error, "no Stampede job context: is this running under a Stampede worker?" if ctx.nil?

      ctx.step(name, cursor: cursor, &)
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
        # backoff_base_ms/backoff_max_ms are here because Stampede::RetryConflict
        # -- the error raised when a job declares retry_on -- tells the user to
        # write exactly this:
        #
        #   stampede max_attempts: 5, backoff_base_ms: 1_000, backoff_max_ms: 300_000
        #
        # They were missing from this list, so following those instructions
        # raised "unknown stampede options: backoff_base_ms, backoff_max_ms".
        # The documented migration away from retry_on did not work.
        unknown = options.keys - %i[max_attempts destination dead_letter_queue
                                    expire_seconds unique unique_period
                                    backoff_base_ms backoff_max_ms]
        raise ArgumentError, "unknown stampede options: #{unknown.join(", ")}" if unknown.any?

        # backoff_base_ms is a smallint on the row. Without this check the
        # failure is a PG::NumericValueOutOfRange reading "smallint out of
        # range" -- raised at ENQUEUE time, which is to say in production,
        # naming neither the option nor the limit. Here it is raised at
        # class-load, once, by the person who wrote it.
        base = options[:backoff_base_ms]
        if base && base.to_i > 32_767
          raise ArgumentError,
                "backoff_base_ms is at most 32767 (smallint on the row), got #{base}. " \
                "It is the FIRST retry delay, which doubles from there; the ceiling " \
                "you want is probably backoff_max_ms, which is an integer."
        end

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
