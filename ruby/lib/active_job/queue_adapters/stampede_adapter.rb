# frozen_string_literal: true

require "active_job"
require "stampede"

module ActiveJob
  module QueueAdapters
    # Stampede adapter.
    #
    # The contract is small: enqueue, enqueue_at, enqueue_all, stopping?.
    #
    # NOTE ON TRANSACTIONS. Rails 8 defaults `enqueue_after_transaction_commit`
    # to false, meaning jobs are enqueued INSIDE the caller's transaction. For a
    # Redis-backed queue that default is a hazard -- the job can be picked up
    # before the transaction commits and find no row -- which is why Sidekiq has
    # warned about it for a decade.
    #
    # Here it is not merely safe, it is the point: the enqueue is an INSERT on
    # the caller's own connection, so it commits atomically with the data that
    # caused it and vanishes if the transaction rolls back. This adapter
    # therefore does NOT opt into after-commit enqueueing.
    class StampedeAdapter < AbstractAdapter
      def initialize(**options)
        super()
        @options = options
        @stopping = false
      end

      def enqueue(job)
        job.provider_job_id = insert(job, nil)
      end

      def enqueue_at(job, timestamp)
        job.provider_job_id = insert(job, Time.at(timestamp).utc)
      end

      # Bulk enqueue. Returns how many were actually created.
      #
      # A deduplicated job that was already queued counts as NOT enqueued, which
      # is what ActiveJob's contract expects, even though the caller's intent
      # was satisfied.
      def enqueue_all(jobs)
        jobs.count do |job|
          id = insert(job, job.scheduled_at && Time.at(job.scheduled_at).utc)
          job.provider_job_id = id
          !id.nil?
        end
      end

      def stopping? = @stopping

      private

      def insert(job, scheduled_at)
        options = job.class.try(:stampede_options) || {}

        Stampede::Client.enqueue(
          # Instance, not class: the class attribute can be a lambda and would
          # stringify into a queue name nothing polls. See Schedule#resolve_queue_name.
          queue: job.queue_name.to_s,
          # The ActiveJob class name is the task. A worker resolves it back to
          # a class, so a rename deployed to the enqueuer but not the workers
          # shows up as `jobs.unknown_task` rather than as silence.
          task: job.class.name,
          args: job.serialize,
          scheduled_at: scheduled_at,
          priority: job.priority,
          max_attempts: options[:max_attempts],
          destination: options[:destination],
          dead_letter_queue: options[:dead_letter_queue],
          expire_seconds: options[:expire_seconds],
          unique: options[:unique]&.then { |u| u.respond_to?(:call) ? u.call(job) : u },
          unique_period: options[:unique_period]&.to_s,
          # The retry_on replacement. Accepted by the DSL but dropped here
          # would be the same bug one layer down: the setting appears to work
          # and the row still carries the default.
          backoff_base_ms: options[:backoff_base_ms],
          backoff_max_ms: options[:backoff_max_ms]
        )
      end
    end
  end
end
