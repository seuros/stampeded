# frozen_string_literal: true

require "timeout"

module Stampeded
  class Worker
    attr_reader :queues, :handlers

    def initialize(queue: nil, queues: nil, except: nil, concurrency: nil, batch_size: nil, poll_interval: nil)
      requested     = queues || queue || Stampeded.config.queues || Stampeded.config.default_queue
      @all_queues   = wildcard?(requested)
      raise ArgumentError, "except: applies only to queues: :all" if except && !@all_queues

      @except       = @all_queues ? Array(except || Stampeded.config.except_queues).flat_map { it.to_s.split(",") }.map(&:strip) : []
      @queues       = @all_queues ? [] : queue_list!(requested)
      @concurrency  = positive!(concurrency || Stampeded.config.concurrency, "concurrency")
      @batch_size   = positive!(batch_size || Stampeded.config.batch_size, "batch_size")
      @poll_interval = poll_interval || Stampeded.config.poll_interval
      @handlers     = {}
      @stopping     = false
      @wake_reader, @wake_writer = IO.pipe
    end

    def queue = @queues.first

    def all_queues? = @all_queues
    def register(task, &block)
      @handlers[task.to_s] = block
      self
    end

    def stop
      @stopping = true
      Stampeded.stopping = true
      @wake_writer.write_nonblock(".", exception: false)
    rescue IOError
      nil
    end

    def stopping? = @stopping
    def run
      require "stampeded/native"
      Native.log_init(library_log_level)
      Stampeded.stopping = false

      puller = Connection.translating_errors { start_puller }
      log "registered #{puller.worker_id} on #{description}"

      @threads = Array.new(@concurrency) { worker_thread(puller) }
      install_signal_handlers
      @wake_reader.wait_readable until stopping?

      deadline = now_monotonic + Stampeded.config.shutdown_timeout
      puller.stop(Stampeded.config.shutdown_timeout)
      stopped = true
      @threads.each { |t| t.join([deadline - now_monotonic, 0].max) }

      stragglers = @threads.count(&:alive?)
      if stragglers.positive?
        log "#{stragglers} job(s) did not finish in #{Stampeded.config.shutdown_timeout}s; " \
            "the reaper will reclaim them"
      end
      log "stopped"
    ensure
      @stopping = true
      begin
        puller.stop(0) if puller && !stopped
      rescue Error => e
        log "could not stop: #{e.message}"
      end
      @threads&.each { |t| t.join(@poll_interval * 2) }
      puller&.close if @threads.nil? || @threads.none?(&:alive?)
      @wake_reader.close
      @wake_writer.close
    end

    private

    def start_puller
      Native::Puller.new(
        url: Connection.url,
        queues: @all_queues ? "*" : @queues.join(","),
        except: @except.any? ? @except.join(",") : nil,
        max_leases: @concurrency,
        batch_size: @batch_size,
        poll_interval: @poll_interval,
        heartbeat_interval: Stampeded.config.heartbeat_interval
      )
    rescue Error => e
      raise SchemaMissing, e if e.message.include?("stampeded_") && e.message.include?("does not exist")

      raise
    end

    def description
      return @queues.join(", ") unless @all_queues

      "every queue#{" except #{@except.join(", ")}" if @except.any?}"
    end

    def worker_thread(puller)
      Thread.new do
        until stopping?
          lease =
            begin
              puller.next(@poll_interval)
            rescue Error => e
              log "claim failed: #{e.message}"
              sleep @poll_interval
              next
            end
          execute(lease, puller.worker_id) if lease
        end
      end
    end

    def execute(lease, worker_id)
      job = Job.new(lease.row, decoded: true)
      started = now_monotonic
      error = nil

      begin
        handler = @handlers[job.task]
        raise UnknownTask, "no handler registered for task #{job.task.inspect}" if handler.nil?

        with_job_timeout(job) { handler.call(JobContext.new(job, worker_id)) }
      rescue Exception => e
        error = e
      end

      report(lease, error)
      log format("%s %s in %.3fs", job.task, job.id, now_monotonic - started)
      raise error if error.is_a?(SignalException) || error.is_a?(SystemExit)
    ensure
      lease.release!
    end

    def report(lease, error)
      return lease.requeue! if error.is_a?(Requeue)

      message = error && error.message.to_s[0, 4000]
      case error && failure_kind_for(error)
      when nil then lease.complete!
      when "unrecoverable" then lease.discard!(message)
      when "timed_out" then lease.timed_out!(message)
      else lease.fail!(message, retry_in_ms: error.is_a?(RetryIn) ? error.delay_ms : nil)
      end
    rescue Error => e
      log "could not report #{lease.row["id"]}: #{e.message}"
    end

    def with_job_timeout(job, &block)
      limit = Stampeded.config.job_timeout
      return yield if limit.nil? || limit <= 0

      expire = (job.row["expire_seconds"] || 3600).to_i
      if limit >= expire
        log "job_timeout (#{limit}s) is not shorter than #{job.task}'s expire_seconds (#{expire}s): it may run twice"
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

    def wildcard?(value)
      return true if value == :all || value.to_s.strip == "*"

      names = value.is_a?(Array) ? value.map(&:to_s) : value.to_s.split(",")
      raise ArgumentError, "\"*\" cannot be mixed with queue names; use except: to leave queues out" if names.map(&:strip).include?("*")

      false
    end

    def queue_list!(value)
      names = value.is_a?(Array) ? value.map(&:to_s) : value.to_s.split(",", -1)
      names = names.map(&:strip)
      raise ArgumentError, "queues must name at least one queue, got #{value.inspect}" if names.empty?
      raise ArgumentError, "queues has a blank entry: #{value.inspect}" if names.any?(&:empty?)

      names.uniq
    end

    def positive!(value, name)
      n = value.to_i
      raise ArgumentError, "#{name} must be at least 1, got #{value.inspect}" if n < 1

      n
    end

    def install_signal_handlers
      %w[INT TERM].each do |sig|
        trap(sig) { stop }
      rescue ArgumentError
        nil
      end
    end

    def now_monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def log? = $stdout.tty? || ENV.key?("STAMPEDED_LOG")

    LOG_LEVELS = %w[trace debug info warn error].freeze

    def library_log_level
      level = ENV.fetch("STAMPEDED_LOG", nil)&.downcase
      return level if LOG_LEVELS.include?(level)

      log? ? "info" : "warn"
    end

    def log(message)
      return unless log?

      warn "[stampeded] #{message}"
    end
  end
end
