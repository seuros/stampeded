# frozen_string_literal: true

module Stampeded
  module QueuePolicy
    module_function

    def set(queue, max_running: nil, max_wait: nil)
      raise Error, "a queue policy needs a queue name" if queue.to_s.strip.empty?

      Connection.translating_errors do
        Connection.with do |c|
          c.exec_params(<<~SQL, [queue.to_s, max_running&.to_i, max_wait&.to_i])
            INSERT INTO stampeded_queue_policies (queue, max_running, max_wait_secs)
            VALUES ($1, $2, $3)
            ON CONFLICT (queue) DO UPDATE SET
              max_running = EXCLUDED.max_running, max_wait_secs = EXCLUDED.max_wait_secs,
              updated_at = now()
          SQL
        end
      end
      queue.to_s
    end

    def remove(queue)
      Connection.with { |c| c.exec_params("DELETE FROM stampeded_queue_policies WHERE queue = $1", [queue.to_s]) }
    end

    def all
      Connection.translating_errors do
        Connection.with do |c|
          c.exec(<<~SQL).to_a
            SELECT queue, max_running, max_wait_secs, verdict, reason, decided_at
              FROM stampeded_queue_policies ORDER BY queue
          SQL
        end
      end
    end
  end
end
