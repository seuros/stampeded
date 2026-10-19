# frozen_string_literal: true

require "json"

module Stampeded
  module Client
    module_function

    def enqueue(queue:, task:, args: {}, meta: {}, scheduled_at: nil, priority: nil,
                max_attempts: nil, tenant_id: nil, unique: nil, unique_period: nil,
                destination: nil, dead_letter_queue: nil, expire_seconds: nil,
                backoff_base_ms: nil, backoff_max_ms: nil)
      sql = <<~SQL
        INSERT INTO stampeded_jobs
          (queue, task, args, tenant_id, priority, max_attempts, scheduled_at,
           dedup_scope, dedup_period, destination, dead_letter_queue, expire_seconds,
           backoff_base_ms, backoff_max_ms, meta)
        VALUES ($1, $2, $3, $4, coalesce($5, 0), coalesce($6, 25), coalesce($7, now()),
                $8, $9, $10, $11, coalesce($12, 3600),
                coalesce($13, 1000), coalesce($14, 3600000), $15)
        ON CONFLICT DO NOTHING
        RETURNING id
      SQL

      params = [
        queue, task, JSON.generate(args.nil? ? {} : args), tenant_id, priority, max_attempts,
        scheduled_at&.then { |t| t.utc.iso8601(6) },
        unique, unique_period, destination, dead_letter_queue, expire_seconds,
        backoff_base_ms, backoff_max_ms, JSON.generate(meta.nil? ? {} : meta)
      ]

      Connection.translating_errors do
        Connection.with { |c| c.exec_params(sql, params) }
                  .then { |r| r.ntuples.zero? ? nil : r[0]["id"] }
      end
    end

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
