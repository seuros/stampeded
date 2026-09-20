# frozen_string_literal: true

require "socket"
# worker.rb classifies Timeout::Error in failure_kind_for. Without this require
# that constant does not exist outside Rails (ActiveSupport loads it), so EVERY
# ordinary job failure raised NameError inside the rescue, report(:fail) never
# ran, and the job sat in `executing` with no last_error until the reaper took
# it -- charging an interruption for what was a clean application error.
require "timeout"

module Stampeded
  # Claims jobs and runs them.
  #
  # Every state transition goes through a SQL function -- `stampeded_claim`,
  # `stampeded_complete`, `stampeded_fail`, `stampeded_touch`. There is no
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
      @queue        = queue || Stampeded.config.default_queue
      @concurrency  = positive!(concurrency || Stampeded.config.concurrency, "concurrency")
      @batch_size   = positive!(batch_size || Stampeded.config.batch_size, "batch_size")
      @poll_interval = poll_interval || Stampeded.config.poll_interval
      @handlers     = {}
      @stopping     = false
      @in_flight    = Concurrent_Set.new
    end

    # A worker with zero concurrency claims nothing, forever.
    #
    # `CONCURRENCY=abc bin/rails stampeded:work` becomes 0 via String#to_i, and
    # the loop then computes `free = 0 - 0` and sleeps in a tight circle
    # doing nothing -- a process that looks healthy, passes every liveness
    # check, and silently runs no jobs. Same shape as a job with
    # max_attempts: 0, and just as hard to spot from the outside.
    def positive!(value, name)
      n = value.to_i
      raise ArgumentError, "#{name} must be at least 1, got #{value.inspect}" if n < 1

      n
    end
    private :positive!

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
      # Fail with an explanation at STARTUP rather than a backtrace on the
      # first claim: a worker that cannot register will never do anything, and
      # the missing schema is the overwhelmingly likely reason.
      worker_id = Connection.translating_errors { register_worker(conn) }
      log "registered #{worker_id} on queue #{@queue}"

      heartbeat = start_heartbeat(worker_id)

      # A FIXED POOL fed through a bounded queue, not a thread per job.
      #
      # The old loop spawned a thread per job and, whenever the pool was full,
      # slept 50 ms before looking again. That put a ceiling on throughput of
      # `concurrency / 0.05` jobs per second regardless of how fast the jobs
      # were: at concurrency 8 that is 160/s, and a load test against 226k real
      # rows measured 138/s for a handler that did nothing at all.
      #
      # The bound on `inbox` IS the backpressure -- pushing blocks while every
      # thread is busy -- so there is no sleep to tune and no ceiling.
      #
      # Persistent threads also make one connection per THREAD possible, where
      # before it was one per JOB: PG.connect + close costs 3.65 ms against
      # 0.2 ms for the query it carried, so 18x the work was connection setup.
      inbox   = SizedQueue.new(@concurrency)
      threads = Array.new(@concurrency) { worker_thread(inbox, worker_id) }

      install_signal_handlers

      until stopping?
        jobs = claim(conn, worker_id, @batch_size)

        if jobs.empty?
          sleep @poll_interval
          next
        end

        # Blocks once every thread is busy. A claimed job waiting here is
        # already `executing` in the database, which is why `inbox` is bounded
        # by concurrency rather than by batch size -- an unbounded handoff
        # would let a big batch sit claimed and un-run while its lease ticked.
        jobs.each { |job| inbox.push(job) }
      end

      # One sentinel per thread, so each wakes and exits rather than blocking
      # on an empty queue forever.
      @concurrency.times { inbox.push(:stop) }
      drain(threads)
      heartbeat.kill
      deregister(conn, worker_id)
      log "stopped"
    ensure
      conn&.close
    end

    private

    def claim(conn, worker_id, limit)
      conn.exec_params("SELECT * FROM stampeded_claim($1, $2, $3)", [@queue, worker_id, limit])
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

    # One long-lived thread, one long-lived reporting connection.
    def worker_thread(inbox, worker_id)
      Thread.new do
        conn = nil
        loop do
          job = inbox.pop
          break if job == :stop

          begin
            # Opened on first use so an idle worker holds nothing, then kept
            # for the life of the thread.
            #
            # RESCUED, and the thread must survive it. Without this a single
            # PG.connect failure -- a failover, a pooler restart, a blip --
            # raised straight out of the loop, hit the ensure, and KILLED THE
            # THREAD with no replacement. The job it had just popped was
            # already `executing` in the database, so it sat there until the
            # reaper. Lose every thread that way and the pool is empty for
            # good: the main loop blocks pushing to a full inbox while the
            # heartbeat keeps reporting a healthy worker -- the same shape as
            # the claim-loop bug fixed earlier, a worker that passes every
            # liveness check and runs nothing.
            conn ||= PG.connect(Connection.connection_params)
            conn = execute(job, worker_id, conn)
          rescue PG::Error => e
            # Drop the probably-dead connection and carry on. The job goes
            # unreported, so the reaper reclaims it -- the right outcome for a
            # worker that could not reach the database.
            log "worker thread: #{e.message}"
            begin
              conn&.close
            rescue StandardError
              nil
            end
            conn = nil
            sleep 0.1
          end
        end
      ensure
        begin
          conn&.close
        rescue StandardError
          nil
        end
      end
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
    def execute(job, worker_id, conn)
      @in_flight.add(job.id)
      started = now_monotonic

      begin
        handler = @handlers[job.task]
        raise UnknownTask, "no handler registered for task #{job.task.inspect}" if handler.nil?

        # Layer 1. See Configuration#job_timeout: without this the database
        # expires a merely-slow job and hands it to another worker while this
        # thread is still running it.
        with_job_timeout(job) { handler.call(JobContext.new(job, worker_id)) }
        conn = report(:complete, job, worker_id, conn: conn)
      rescue Exception => e # rubocop:disable Lint/RescueException
        # Exception, not StandardError: a job that raises SignalException or
        # NoMemoryError must still be reported, or it sits in `executing` until
        # the reaper's dead_after elapses. Re-raised below if it is not ours to
        # swallow.
        conn = report(:fail, job, worker_id, conn: conn, error: e)
        raise if e.is_a?(SignalException) || e.is_a?(SystemExit)
      ensure
        @in_flight.delete(job.id)
        log format("%s %s in %.3fs", job.task, job.id, now_monotonic - started)
      end

      conn
    end

    # Reports on the CALLER'S connection.
    #
    # This used to be `Connection.own`, which opens a fresh PG connection and
    # closes it -- once per job. Measured at 3.65 ms against 0.2 ms for the
    # query it carried, so 18x the work was connection setup. Returns the
    # connection so a reconnect can be handed back to the thread.
    def report(outcome, job, worker_id, conn:, error: nil)
      c = conn
      case outcome
        when :complete
          r = c.exec_params("SELECT * FROM stampeded_complete($1, $2, $3)",
                            [job.queue, job.id, worker_id])
          verify_transition(r, :complete)
        when :fail
          kind = failure_kind_for(error)
          # The retry DELAY is computed by the database, not here: passing an
          # absolute timestamp would make retry timing depend on this box's
          # clock agreeing with the database's.
          #
          # A handler may override it by raising RetryIn -- the Stampeded
          # equivalent of retry_on(wait:), and the reason refusing retry_on
          # costs nothing in expressiveness. It picks WHEN, not whether: the
          # attempt is charged and max_attempts still applies.
          delay =
            if kind == "unrecoverable"
              nil
            elsif error.is_a?(RetryIn)
              error.delay_ms
            else
              backoff_ms(job)
            end
          r = c.exec_params(
            "SELECT * FROM stampeded_fail($1, $2, $3, $4, $5, $6, $7)",
            [job.queue, job.id, worker_id, kind, error.message.to_s[0, 4000], delay, nil]
          )
          # Which transition SQL should have made depends on whether there was
          # budget left, and SQL decides that -- so check against what it
          # reported rather than against what we guessed.
        verify_transition(r, r.ntuples.positive? && r[0]["now_state"] == "discarded" ? :discard : :retry)
      end
      conn
    rescue PG::Error => e
      # Losing the report is survivable: the reaper reclaims the job once this
      # worker stops heartbeating. Losing the WORKER over it is not.
      #
      # The connection may be the casualty, though, and it is now reused for
      # every subsequent job on this thread -- so replace it rather than hand
      # back a dead socket.
      log "could not report #{outcome} for #{job.id}: #{e.message}"
      reconnect(conn)
      conn
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
          "does not declare. schema/schema.sql and the state machine disagree."
    end

    # Bound the handler, and warn if the bound cannot do its job.
    #
    # `expire_seconds` is per-job and set by the enqueuer, so the ordering that
    # prevents double execution cannot be enforced by a constant -- only
    # checked against the row we were actually handed.
    def with_job_timeout(job, &block)
      limit = Stampeded.config.job_timeout
      return yield if limit.nil? || limit <= 0

      expire = (job.row["expire_seconds"] || 3600).to_i
      if limit >= expire
        log "job_timeout (#{limit}s) is not shorter than #{job.task}'s expire_seconds " \
            "(#{expire}s): the database may expire this job and let another worker " \
            "claim it while this one is still running it"
      end

      Timeout.timeout(limit, Timeout::Error, "job exceeded job_timeout of #{limit}s", &block)
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
        "INSERT INTO stampeded_workers (hostname, pid, queues) VALUES ($1, $2, $3) RETURNING id",
        [Socket.gethostname, Process.pid, "{#{@queue}}"]
      )[0]["id"]
    end

    def deregister(conn, worker_id)
      conn.exec_params("DELETE FROM stampeded_workers WHERE id = $1", [worker_id])
    rescue PG::Error
      nil
    end

    def start_heartbeat(worker_id)
      Thread.new do
        Connection.own do |c|
          loop do
            sleep Stampeded.config.heartbeat_interval
            begin
              c.exec_params("SELECT stampeded_heartbeat($1)", [worker_id])
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
      deadline = now_monotonic + Stampeded.config.shutdown_timeout
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
      log "#{stragglers} job(s) did not finish in #{Stampeded.config.shutdown_timeout}s; " \
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
      return unless $stdout.tty? || ENV["STAMPEDED_LOG"]

      warn "[stampeded] #{message}"
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
