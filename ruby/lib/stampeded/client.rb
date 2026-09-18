# frozen_string_literal: true

require "json"

module Stampeded
  # Enqueue, and the handful of reads that do not belong to the worker.
  module Client
    module_function

    # rubocop:disable Metrics/ParameterLists
    def enqueue(queue:, task:, args: {}, scheduled_at: nil, priority: nil,
                max_attempts: nil, tenant_id: nil, unique: nil, unique_period: nil,
                destination: nil, dead_letter_queue: nil, expire_seconds: nil,
                backoff_base_ms: nil, backoff_max_ms: nil)
      # A plain INSERT. Not a stored procedure, because this is exactly what a
      # caller in any language writes with their own driver -- and because
      # running it on the caller's connection is what makes the enqueue
      # transactional (see Connection).
      sql = <<~SQL
        INSERT INTO stampeded_jobs
          (queue, task, args, tenant_id, priority, max_attempts, scheduled_at,
           dedup_scope, dedup_period, destination, dead_letter_queue, expire_seconds,
           backoff_base_ms, backoff_max_ms)
        VALUES ($1, $2, $3, $4, coalesce($5, 0), coalesce($6, 25), coalesce($7, now()),
                $8, $9, $10, $11, coalesce($12, 3600),
                coalesce($13, 1000), coalesce($14, 3600000))
        ON CONFLICT DO NOTHING
        RETURNING id
      SQL

      params = [
        queue, task, JSON.generate(args), tenant_id, priority, max_attempts,
        scheduled_at&.then { |t| t.utc.iso8601(6) },
        unique, unique_period, destination, dead_letter_queue, expire_seconds,
        backoff_base_ms, backoff_max_ms
      ]

      # nil rather than raising when a deduplicated job was already queued: the
      # caller's intent ("ensure this job exists") was satisfied, and making it
      # an error forces every caller to tell "already queued" apart from
      # "failed".
      Connection.translating_errors do
        Connection.with { |c| c.exec_params(sql, params) }
                  .then { |r| r.ntuples.zero? ? nil : r[0]["id"] }
      end
    end
    # rubocop:enable Metrics/ParameterLists

    def fetch(queue, id)
      Connection.translating_errors do
        Connection.with do |c|
          r = c.exec_params("SELECT * FROM stampeded_jobs WHERE queue = $1 AND id = $2", [queue, id])
          r.ntuples.zero? ? nil : Job.new(r[0])
        end
      end
    end
  end
end
