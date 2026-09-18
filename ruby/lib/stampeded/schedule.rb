# frozen_string_literal: true

require "json"

module Stampeded
  # Recurring jobs.
  #
  # The schedule row IS the due row and its row lock IS the leader election, so
  # there is nothing to coordinate here -- the Rust daemon's scheduler fires
  # these, and any number of daemons may tick concurrently. This class only
  # writes the row.
  #
  # The subtlety that makes it work: the daemon copies the schedule's `args`
  # verbatim onto the job it enqueues, and it has no idea what ActiveJob is. So
  # the args stored here must ALREADY be a valid ActiveJob payload, or the
  # worker gets a job it cannot deserialise. Discovered by firing one and
  # watching both occurrences get discarded.
  module Schedule
    module_function

    # Register (or update) a recurring ActiveJob job.
    #
    #   Stampeded::Schedule.recurring(
    #     key: "nightly-report",
    #     job: ReportJob,
    #     cron: "0 3 * * *",
    #     args: [42],
    #     timezone: "America/New_York"
    #   )
    #
    # `dst_policy` defaults to nil, which lets the database keep whatever the
    # expression implies -- an expression naming its hours wants wall_clock
    # ("the 03:00 report" runs once on a fall-back day); one using a step wants
    # instant ("every two hours" should run 13 times on a 25-hour day).
    def recurring(key:, job:, cron:, args: [], queue: nil, timezone: "UTC",
                  dst_policy: nil, priority: nil)
      raise Error, "a schedule needs a key: it is how you pause, remove or find it" if key.to_s.strip.empty?

      validate_cron!(cron)
      job_class = job.is_a?(Class) ? job : job.class
      queue ||= resolve_queue_name(job_class) || Stampeded.config.default_queue

      payload = active_job_payload(job_class, args, queue, priority)

      Connection.with do |c|
        c.exec_params(<<~SQL, [key, queue, job_class.name, JSON.generate(payload), cron, timezone, dst_policy, priority])
          INSERT INTO stampeded_schedules
            (key, queue, task, args, cron_expr, timezone, dst_policy, priority, next_run_at)
          VALUES ($1, $2, $3, $4, $5, $6,
                  coalesce($7, 'instant'), coalesce($8, 0), now())
          ON CONFLICT (key) DO UPDATE SET
            queue = EXCLUDED.queue, task = EXCLUDED.task, args = EXCLUDED.args,
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

    # Reject an expression the daemon will not be able to parse.
    #
    # The Rust side validates on write (`upsert_cron_schedule`) so a typo is a
    # rejected call. Ruby did not, so `cron: "garbage"` was stored happily and
    # then failed at FIRE time -- where the scheduler logs a warning, leaves
    # the row due, and repeats that every tick forever. A schedule that never
    # runs plus a log line every five seconds.
    #
    # This is a shape check, not the full parser: five fields, each made of
    # the characters the grammar allows. It catches the mistakes people
    # actually make -- wrong field count, prose, a stray semicolon -- without
    # reimplementing cron in two languages, which would be a second thing to
    # keep in sync.
    CRON_FIELD = /\A[-0-9*,\/a-zA-Z]+\z/
    private_constant :CRON_FIELD

    def validate_cron!(expr)
      fields = expr.to_s.split
      return if fields.size == 5 && fields.all? { |f| f.match?(CRON_FIELD) }

      raise Error, <<~MSG
        #{expr.inspect} is not a cron expression.

        Five fields are required: minute hour day-of-month month day-of-week

            "0 3 * * *"        every day at 03:00
            "*/15 * * * *"     every fifteen minutes
            "0 9-17 * * mon-fri"
      MSG
    end

    # ActiveJob's `queue_name` CLASS attribute may hold a lambda -- Rails sets
    # the default that way, and it is the INSTANCE that resolves it. Reading it
    # off the class and stringifying gives "#<Proc:0x...queue_name.rb:55>",
    # which is a perfectly valid queue name that no worker will ever poll. The
    # recurring job then sits in an unreachable queue forever, with nothing to
    # indicate anything is wrong.
    #
    # Found by firing one and watching it land in a queue named after a lambda.
    def resolve_queue_name(job_class)
      return nil unless job_class.respond_to?(:queue_name)

      name = job_class.queue_name
      name = job_class.new.queue_name if name.respond_to?(:call)
      name&.to_s
    rescue StandardError
      nil
    end

    # An ActiveJob payload with NO job_id.
    #
    # Deliberate: the daemon copies these args onto every firing, so a stored
    # job_id would make every occurrence of a nightly job share one id --
    # ActiveJob tolerates it, but the logs and any tracing built on job_id
    # become useless. The worker fills it in from the row's own uuid instead,
    # which also makes the ActiveJob id and the Stampeded id the same value.
    def active_job_payload(job_class, args, queue, priority)
      {
        "job_class" => job_class.name,
        "queue_name" => queue,
        "priority" => priority,
        "arguments" => args,
        "executions" => 0,
        "exception_executions" => {},
        "locale" => defined?(I18n) ? I18n.locale.to_s : "en",
        "timezone" => defined?(Time.zone) && Time.zone ? Time.zone.name : "UTC",
        "enqueued_at" => nil
      }
    end
  end
end
