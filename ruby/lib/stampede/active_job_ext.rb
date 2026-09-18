# frozen_string_literal: true

module Stampede
  # Class-level DSL for the Stampede-specific columns ActiveJob has no concept
  # of, plus the retry_on guard.
  module ActiveJobExt
    def self.included(base)
      base.extend(ClassMethods)
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
