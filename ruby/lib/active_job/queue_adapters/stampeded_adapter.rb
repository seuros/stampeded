# frozen_string_literal: true

require "active_job"
require "stampeded"

module ActiveJob
  module QueueAdapters
    class StampededAdapter < AbstractAdapter
      def initialize(**options)
        super()
        @options = options
      end

      def enqueue(job)
        job.provider_job_id = insert(job, nil)
      end

      def enqueue_at(job, timestamp)
        job.provider_job_id = insert(job, Time.at(timestamp).utc)
      end

      def enqueue_all(jobs)
        jobs.count do |job|
          id = insert(job, job.scheduled_at && Time.at(job.scheduled_at).utc)
          job.provider_job_id = id
          !id.nil?
        end
      end

      def stopping? = Stampeded.stopping?

      private

      def insert(job, scheduled_at)
        options = job.class.try(:stampeded_options) || {}

        task, args, meta = Stampeded::Envelope.from_active_job(job)

        Stampeded::Client.enqueue(
          queue: job.queue_name.to_s,
          task: task,
          args: args,
          meta: meta,
          scheduled_at: scheduled_at,
          priority: job.priority,
          max_attempts: options[:max_attempts],
          destination: options[:destination],
          dead_letter_queue: options[:dead_letter_queue],
          expire_seconds: options[:expire_seconds],
          unique: options[:unique]&.then { |u| u.respond_to?(:call) ? u.call(job) : u },
          unique_period: options[:unique_period]&.to_s,
          backoff_base_ms: options[:backoff_base_ms],
          backoff_max_ms: options[:backoff_max_ms]
        )
      end
    end
  end
end
