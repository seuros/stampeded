# frozen_string_literal: true

require "json"

module Stampeded
  module Schedule
    module_function

    # SQLSTATEs stampeded_sync_schedules raises for a declaration it refuses.
    REFUSED = %w[SD409 22023].freeze

    def recurring(key:, job:, cron:, args: [], queue: nil, timezone: "UTC",
                  dst_policy: nil, priority: nil, owner: nil)
      owner = check_owner!(owner) unless owner.nil?
      row = definition(key:, job:, cron:, args:, queue:, timezone:, dst_policy:, priority:)
      params = row.values_at("key", "queue", "task", "args", "meta", "cron_expr", "timezone", "dst_policy", "priority")
      params[3] = JSON.generate(params[3])
      params[4] = JSON.generate(params[4])

      Connection.with do |c|
        next c.exec_params(<<~SQL, params) if owner.nil?
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

        written = c.exec_params(<<~SQL, [*params, owner])
          INSERT INTO stampeded_schedules
            (key, queue, task, args, meta, cron_expr, timezone, dst_policy, priority, next_run_at, owner)
          VALUES ($1, $2, $3, $4, $5, $6, $7,
                  $8, coalesce($9, 0), now(), $10)
          ON CONFLICT (key) DO UPDATE SET
            queue = EXCLUDED.queue, task = EXCLUDED.task, args = EXCLUDED.args, meta = EXCLUDED.meta,
            cron_expr = EXCLUDED.cron_expr, interval_secs = NULL,
            timezone = EXCLUDED.timezone, dst_policy = EXCLUDED.dst_policy,
            priority = EXCLUDED.priority, owner = EXCLUDED.owner, updated_at = now()
          WHERE stampeded_schedules.owner IS NULL OR stampeded_schedules.owner = EXCLUDED.owner
          RETURNING key
        SQL
        if written.ntuples.zero?
          holder = c.exec_params("SELECT owner FROM stampeded_schedules WHERE key = $1", [key])[0]&.fetch("owner")
          raise Error, "schedule #{key.inspect} belongs to owner #{holder.inspect}, not #{owner.inspect}"
        end
      end
      key
    end

    # Makes owner's schedules exactly these, in one transaction; other owners' rows are never touched.
    # Returns { key => :created | :updated | :adopted | :removed } for what changed.
    def sync(schedules, owner:)
      owner = check_owner!(owner)
      rows = Array(schedules).map { |s| definition(**s) }

      Connection.with do |c|
        c.exec_params("SELECT schedule_key, change FROM stampeded_sync_schedules($1, $2::jsonb)",
                      [owner, JSON.generate(rows)])
         .to_h { |r| [r["schedule_key"], r["change"].to_sym] }
      end
    rescue PG::Error => e
      raise unless REFUSED.include?(e.result&.error_field(PG::PG_DIAG_SQLSTATE))

      raise Error, e.result.error_field(PG::PG_DIAG_MESSAGE_PRIMARY)
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
      # Through to_jsonb, so a schema without owners reads nil.
      Connection.with do |c|
        c.exec("SELECT key, queue, task, cron_expr, timezone, dst_policy, paused, next_run_at, last_run_at,
                       to_jsonb(s) ->> 'owner' AS owner
                  FROM stampeded_schedules s ORDER BY next_run_at").to_a
      end
    end

    # One schedule as stampeded_sync_schedules reads it; validated here so a
    # bad expression is refused before anything is written.
    def definition(key:, job:, cron:, args: [], queue: nil, timezone: "UTC", dst_policy: nil,
                   priority: nil, max_attempts: nil, max_interruptions: nil, expire_seconds: nil)
      raise Error, "a schedule needs a key" if key.to_s.strip.empty?

      suggested = check_cron!(cron, timezone)
      dst_policy ||= suggested
      job_class = job.is_a?(Class) ? job : job.class
      # As `perform` takes them: a Hash is keywords, anything else positional.
      arguments = args.is_a?(Hash) ? [Hash.ruby2_keywords_hash(args)] : Array(args)
      queue ||= resolve_queue_name(job_class, arguments) || Stampeded.config.default_queue

      task, standard_args, meta = envelope(job_class, arguments, queue, priority)
      {
        "key" => key.to_s, "queue" => queue, "task" => task, "args" => standard_args, "meta" => meta,
        "cron_expr" => cron, "timezone" => timezone, "dst_policy" => dst_policy, "priority" => priority,
        "max_attempts" => max_attempts, "max_interruptions" => max_interruptions,
        "expire_seconds" => expire_seconds
      }
    end

    def check_owner!(owner)
      raise Error, "a schedule owner must be a non-blank string" if owner.to_s.strip.empty?

      owner.to_s
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

    # A `queue_as` block sees the schedule's arguments, which are the same every run.
    def resolve_queue_name(job_class, arguments)
      return nil unless job_class.respond_to?(:queue_name)

      name = job_class.queue_name
      name = job_class.new(*arguments).queue_name if name.respond_to?(:call)
      name&.to_s
    rescue StandardError => e
      raise Error, "#{job_class.name}'s queue_as raised #{e.class}: #{e.message}; " \
                   "pass queue: to name the schedule's queue"
    end

    def envelope(job_class, arguments, queue, priority)
      context = {
        "job_class" => job_class.name,
        "queue_name" => queue,
        "priority" => priority,
        "locale" => defined?(I18n) ? I18n.locale.to_s : "en",
        "timezone" => defined?(Time.zone) && Time.zone ? Time.zone.name : "UTC"
      }

      Envelope.build(job_class, arguments, context)
    end
  end
end
