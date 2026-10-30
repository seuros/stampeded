# frozen_string_literal: true

require "json"

module Stampeded
  module Schedule
    module_function

    def recurring(key:, job:, cron:, args: [], queue: nil, timezone: "UTC",
                  dst_policy: nil, priority: nil)
      raise Error, "a schedule needs a key" if key.to_s.strip.empty?

      suggested = check_cron!(cron, timezone)
      dst_policy ||= suggested
      job_class = job.is_a?(Class) ? job : job.class
      queue ||= resolve_queue_name(job_class) || Stampeded.config.default_queue

      task, standard_args, meta = envelope(job_class, args, queue, priority)

      Connection.with do |c|
        c.exec_params(<<~SQL, [key, queue, task, JSON.generate(standard_args), JSON.generate(meta), cron, timezone, dst_policy, priority])
          INSERT INTO stampeded_schedules
            (key, queue, task, args, meta, cron_expr, timezone, dst_policy, priority, next_run_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7,
                  $8, coalesce($9, 0), now())
          ON CONFLICT (key) DO UPDATE SET
            queue = EXCLUDED.queue, task = EXCLUDED.task, args = EXCLUDED.args, meta = EXCLUDED.meta,
            cron_expr = EXCLUDED.cron_expr, interval_secs = NULL,
            timezone = EXCLUDED.timezone, dst_policy = EXCLUDED.dst_policy,
            priority = EXCLUDED.priority, updated_at = now()
        SQL
      end
      key
    end

    def remove(key)
      Connection.with { |c| c.exec_params("DELETE FROM stampeded_schedules WHERE key = $1", [key]) }
    end

    def pause(key, paused: true)
      Connection.with do |c|
        c.exec_params("UPDATE stampeded_schedules SET paused = $2, updated_at = now() WHERE key = $1",
                      [key, paused])
      end
    end

    def all
      Connection.with do |c|
        c.exec("SELECT key, queue, task, cron_expr, timezone, dst_policy, paused, next_run_at, last_run_at
                  FROM stampeded_schedules ORDER BY next_run_at").to_a
      end
    end

    def check_cron!(expr, timezone)
      require "stampeded/native"
      Native.cron_check(expr, timezone)
    rescue Error => e
      raise Error, e.message unless e.message.include?("cron")

      raise Error, <<~MSG
        #{expr.inspect}: #{e.message}

        Five fields are required: minute hour day-of-month month day-of-week

            "0 3 * * *"        every day at 03:00
            "*/15 * * * *"     every fifteen minutes
            "0 9-17 * * mon-fri"
      MSG
    end

    def resolve_queue_name(job_class)
      return nil unless job_class.respond_to?(:queue_name)

      name = job_class.queue_name
      name = job_class.new.queue_name if name.respond_to?(:call)
      name&.to_s
    rescue StandardError
      nil
    end

    # `args` as `recurring` takes them: a Hash is keywords, anything else positional.
    def envelope(job_class, args, queue, priority)
      context = {
        "job_class" => job_class.name,
        "queue_name" => queue,
        "priority" => priority,
        "locale" => defined?(I18n) ? I18n.locale.to_s : "en",
        "timezone" => defined?(Time.zone) && Time.zone ? Time.zone.name : "UTC"
      }

      arguments = args.is_a?(Hash) ? [Hash.ruby2_keywords_hash(args)] : Array(args)
      Envelope.build(job_class, arguments, context)
    end
  end
end
