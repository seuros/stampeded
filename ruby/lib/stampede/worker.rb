# frozen_string_literal: true

require "socket"

module Stampede
  # Claims jobs and runs them.
  #
  # Every state transition goes through a SQL function -- `stampede_claim`,
  # `stampede_complete`, `stampede_fail`, `stampede_touch`. There is no
  # Ruby-side claim query, no Ruby-side attempt arithmetic and no Ruby-side
  # state machine, because the Rust worker calls the same four functions and a
  # second implementation would drift (plans/05, plans/16 §5).
  #
  # THREADS. Rails jobs are overwhelmingly IO-bound and the GVL is released for
  # database calls, HTTP and file IO. CPU-bound jobs will not scale on this
  # pool -- run more processes with a smaller pool each, which is the same
  # advice Sidekiq gives. solid_queue forks instead and pays for a supervisor.
  class Worker
    attr_reader :queue, :handlers

    def initialize(queue: nil, concurrency: nil, batch_size: nil, poll_interval: nil)
      @queue        = queue || Stampede.config.default_queue
      @concurrency  = concurrency || Stampede.config.concurrency
      @batch_size   = batch_size || Stampede.config.batch_size
      @poll_interval = poll_interval || Stampede.config.poll_interval
      @handlers     = {}
      @stopping     = false
      @in_flight    = Concurrent_Set.new
    end

    # Register a handler for a task name.
    #
    # ActiveJob does not need this -- the adapter registers a single handler
    # that dispatches on the serialised job class. It is here for plain Ruby
    # jobs that want the queue without the framework.
    def register(task, &block)
      @handlers[task.to_s] = block
      self
    end

    def stop
      @stopping = true
    end

    def stopping? = @stopping

    # Run until stopped.
    def run
      # Through Connection, NOT PG.connect(config.database_url). Inside Rails
      # the app's database.yml is authoritative and usually has no url at all,
      # so dialing an env var connects somewhere else -- which is exactly what
      # happened: the worker claimed from a stale database, ran a job that
      # lived there, and the app's own job sat `available` forever with nothing
      # reporting an error.
      conn = PG.connect(Connection.connection_params)
      worker_id = register_worker(conn)
      log "registered #{worker_id} on queue #{@queue}"

      heartbeat = start_heartbeat(worker_id)
      pool      = []

      install_signal_handlers

      until stopping?
        pool.reject! { |t| !t.alive? }
        free = @concurrency - pool.size

        if free <= 0
          sleep 0.05
          next
        end

        jobs = claim(conn, worker_id, [free, @batch_size].min)

        if jobs.empty?
          sleep @poll_interval
          next
        end

        jobs.each do |job|
          pool << Thread.new { execute(job, worker_id) }
        end
      end

      drain(pool)
      heartbeat.kill
      deregister(conn, worker_id)
      log "stopped"
    ensure
      conn&.close
    end

    private

    def claim(conn, worker_id, limit)
      conn.exec_params("SELECT * FROM stampede_claim($1, $2, $3)", [@queue, worker_id, limit])
          .map { |row| Job.new(row) }
    rescue PG::Error => e
      # RECONNECT, do not just retry.
      #
      # Without this the worker rescued the error, slept, and retried on the
      # SAME DEAD CONNECTION -- forever. The thread stayed alive and the
      # process stayed up, so every liveness check passed while the worker
      # silently did nothing. Any failover, pooler restart, admin
      # `pg_terminate_backend` or network blip wedged it permanently.
      #
      # Verified: killing the worker's backends left it running and it
      # executed zero further jobs.
      log "claim failed: #{e.message}"
      reconnect(conn)
      sleep @poll_interval
      []
    end

    # Bring a connection back after the server closed it.
    #
    # PG::Connection#reset re-establishes using the original parameters, which
    # is what makes this possible without plumbing the config back through.
    def reconnect(conn)
      return if conn.nil?
      return if conn.status == PG::CONNECTION_OK

      log "reconnecting"
      conn.reset
      log "reconnected"
    rescue PG::Error => e
      # Still down. The next poll tries again -- this is a blip, not a reason
      # to take the worker out of service.
      log "reconnect failed: #{e.message}"
    end

    # One job, on its own connection.
    #
    # A connection per in-flight job rather than per worker, because the job
    # itself will want one (ActiveRecord checks one out) and sharing this one
    # would serialise the pool behind whichever job is reporting. It is
    # released as soon as the outcome is written -- NOT held for the duration
    # of the job, which is underway's documented trap: a pinned pooled
    # connection per in-flight job exhausts max_connections with a handful of
    # processes.
    def execute(job, worker_id)
      @in_flight.add(job.id)
      started = now_monotonic

      begin
        handler = @handlers[job.task]
        raise UnknownTask, "no handler registered for task #{job.task.inspect}" if handler.nil?

        handler.call(JobContext.new(job, worker_id))
        report(:complete, job, worker_id)
      rescue Exception => e # rubocop:disable Lint/RescueException
        # Exception, not StandardError: a job that raises SignalException or
        # NoMemoryError must still be reported, or it sits in `executing` until
        # the reaper's dead_after elapses. Re-raised below if it is not ours to
        # swallow.
        report(:fail, job, worker_id, error: e)
        raise if e.is_a?(SignalException) || e.is_a?(SystemExit)
      ensure
        @in_flight.delete(job.id)
        log format("%s %s in %.3fs", job.task, job.id, now_monotonic - started)
      end
    end

    def report(outcome, job, worker_id, error: nil)
      Connection.own do |c|
        case outcome
        when :complete
          r = c.exec_params("SELECT * FROM stampede_complete($1, $2, $3)",
                            [job.queue, job.id, worker_id])
          verify_transition(r, :complete)
        when :fail
          kind = failure_kind_for(error)
          # The retry DELAY is computed by the database, not here: passing an
          # absolute timestamp would make retry timing depend on this box's
          # clock agreeing with the database's.
          delay = kind == "unrecoverable" ? nil : backoff_ms(job)
          r = c.exec_params(
            "SELECT * FROM stampede_fail($1, $2, $3, $4, $5, $6, $7)",
            [job.queue, job.id, worker_id, kind, error.message.to_s[0, 4000], delay, nil]
          )
          # Which transition SQL should have made depends on whether there was
          # budget left, and SQL decides that -- so check against what it
          # reported rather than against what we guessed.
          verify_transition(r, r.ntuples.positive? && r[0]["now_state"] == "discarded" ? :discard : :retry)
        end
      end
    rescue PG::Error => e
      # Losing the report is survivable: the reaper reclaims the job once this
      # worker stops heartbeating. Losing the WORKER over it is not.
      log "could not report #{outcome} for #{job.id}: #{e.message}"
    end

    # Check what SQL actually did against the declared lifecycle.
    #
    # It cannot prevent anything -- SQL is authoritative and a polyglot worker
    # never runs this code. It exists to make drift LOUD: if a WHERE clause is
    # edited and starts permitting a transition Lifecycle forbids, the
    # alternative to this warning is finding out from a customer.
    def verify_transition(result, event)
      return if result.ntuples.zero?

      was = result[0]["was"]
      now = result[0]["now_state"]
      return if was.nil? || now.nil?
      return if Lifecycle.verify(was, now, event)

      log "DRIFT: the database did #{was} -> #{now} on #{event}, which lifecycle.rb " \
          "does not declare. schema/002_operations.sql and the state machine disagree."
    end

    def failure_kind_for(error)
      case error
      when UnknownTask then "unrecoverable"
      when Timeout::Error then "timed_out"
      else "raised"
      end
    end

    # Exponential with full jitter, capped -- the same shape as the Rust
    # worker's chrono-machines policy, and read from the row so both sides use
    # the schedule the dashboard renders.
    def backoff_ms(job)
      base = (job.row["backoff_base_ms"] || 1_000).to_i
      cap  = (job.row["backoff_max_ms"] || 3_600_000).to_i
      ceiling = [base * (2**[job.attempt - 1, 0].max), cap].min
      rand(0..ceiling)
    end

    def register_worker(conn)
      conn.exec_params(
        "INSERT INTO stampede_workers (hostname, pid, queues) VALUES ($1, $2, $3) RETURNING id",
        [Socket.gethostname, Process.pid, "{#{@queue}}"]
      )[0]["id"]
    end

    def deregister(conn, worker_id)
      conn.exec_params("DELETE FROM stampede_workers WHERE id = $1", [worker_id])
    rescue PG::Error
      nil
    end

    def start_heartbeat(worker_id)
      Thread.new do
        Connection.own do |c|
          loop do
            sleep Stampede.config.heartbeat_interval
            begin
              c.exec_params("SELECT stampede_heartbeat($1)", [worker_id])
            rescue PG::Error => e
              # Do not die: a transient blip should not take the worker down,
              # and the reaper is the backstop if it is not transient.
              #
              # But DO reconnect, for the same reason the claim loop does --
              # otherwise a dropped connection stops the heartbeat forever, the
              # reaper reclaims this worker's jobs as orphans, and the worker
              # keeps claiming more that it will also appear to abandon.
              log "heartbeat failed: #{e.message}"
              reconnect(c)
            end
          end
        end
      end
    end

    # Let in-flight jobs finish.
    #
    # plans/15: a graceful shutdown must NOT consume the interruption budget.
    # Draining is how that is achieved -- each job reports its own outcome
    # normally, so nothing is ever recorded as a worker death. Killing the
    # threads here would charge every in-flight job an interruption for a
    # routine deploy.
    def drain(pool)
      deadline = now_monotonic + Stampede.config.shutdown_timeout
      log "draining #{pool.count(&:alive?)} in-flight job(s)"

      pool.each do |t|
        remaining = deadline - now_monotonic
        break if remaining <= 0

        t.join(remaining)
      end

      stragglers = pool.count(&:alive?)
      return if stragglers.zero?

      # These will be reclaimed by the reaper and CHARGED an interruption,
      # which is correct: they outlived the shutdown window, so from the
      # queue's point of view this process did die on them.
      log "#{stragglers} job(s) did not finish in #{Stampede.config.shutdown_timeout}s; " \
          "the reaper will reclaim them"
    end

    def install_signal_handlers
      %w[INT TERM].each do |sig|
        trap(sig) { stop }
      rescue ArgumentError
        nil # not all signals are trappable everywhere
      end
    end

    def now_monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def log(message)
      return unless $stdout.tty? || ENV["STAMPEDE_LOG"]

      warn "[stampede] #{message}"
    end

    # A tiny mutex-guarded set, so the gem does not depend on concurrent-ruby
    # for one collection.
    class Concurrent_Set # rubocop:disable Naming/ClassAndModuleCamelCase
      def initialize
        @set = {}
        @mutex = Mutex.new
      end

      def add(x)    = @mutex.synchronize { @set[x] = true }
      def delete(x) = @mutex.synchronize { @set.delete(x) }
      def size      = @mutex.synchronize { @set.size }
    end
  end
end
