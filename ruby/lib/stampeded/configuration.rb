# frozen_string_literal: true

module Stampeded
  class Configuration
    # Where the jobs live. Defaults to ActiveRecord's connection when Rails is
    # present, so enqueueing participates in the caller's transaction.
    attr_accessor :database_url

    # Default queue for `Stampeded.enqueue` and for ActiveJob jobs that do not
    # set one.
    attr_accessor :default_queue

    # Worker pool size.
    #
    # THREADS, not processes. Rails jobs are overwhelmingly IO-bound -- a
    # database call, an HTTP call, an email -- and the GVL is released for all
    # of those. CPU-bound jobs will NOT scale on this pool; run more processes
    # with a smaller pool each, which is the same advice Sidekiq gives.
    attr_accessor :concurrency

    # How many jobs to claim per round trip.
    attr_accessor :batch_size

    # Poll floor. Never disabled: a redundant poll costs one empty query, a
    # missed wakeup costs a stuck job.
    attr_accessor :poll_interval

    # Worker heartbeat. Must be comfortably under the daemon's `dead_after`
    # (default 5 minutes) or healthy workers get reaped mid-job.
    attr_accessor :heartbeat_interval

    # How long to let in-flight jobs finish on shutdown before giving up.
    attr_accessor :shutdown_timeout

    # Which ActiveRecord class owns the queue's connection.
    #
    # A string, resolved lazily, so an initializer can name a class that is not
    # loaded yet. Default: ActiveRecord::Base, i.e. the primary database.
    #
    # Set it when the queue lives in a SEPARATE database, which Rails 8
    # encourages and which GoodJob and solid_queue both default to:
    #
    #   class StampededRecord < ApplicationRecord
    #     self.abstract_class = true
    #     connects_to database: { writing: :jobs, reading: :jobs }
    #   end
    #
    #   Stampeded.config.connection_class = "StampededRecord"
    #
    # KNOW WHAT THIS COSTS. Stampeded's headline property is that enqueue is an
    # INSERT on the caller's own connection, so it commits or rolls back with
    # the work that created it. A separate database is a separate connection,
    # and that property is gone: the job can commit while the transaction that
    # created it rolls back. GoodJob and solid_queue have the same hole and it
    # is the reason they need a "wait for the record to exist" dance.
    #
    # Keep the queue in the primary database unless you have a specific reason
    # not to. The reason is usually schema tidiness, and transactional enqueue
    # is worth more than that.
    attr_accessor :connection_class

    # Per-job execution timeout -- layer 1 of the three in plans/15, and the
    # Ruby counterpart of the Rust worker's `job_timeout`.
    #
    # This is not a nicety. Layer 2 (`expire_seconds`, database-enforced,
    # default 1 hour) fires whether or not the worker is alive, and routes the
    # job back to `retryable` where ANOTHER worker claims it. Without a layer-1
    # timeout that is shorter, a job that merely hangs -- an HTTP call with no
    # timeout is the usual cause -- keeps running in this process while a second
    # worker starts it again. That is the double-execution mechanism River
    # guards with `rescue_after > job_timeout`, and with no layer 1 at all the
    # invariant was violated by construction rather than by misconfiguration.
    #
    # Advisory, like every in-process timeout: Ruby cannot interrupt a blocking
    # C call, exactly as tokio cannot preempt a blocking task. It covers the
    # common case, and layers 2 and 3 cover the rest.
    attr_accessor :job_timeout

    def initialize
      @database_url       = ENV.fetch("STAMPEDED_DATABASE_URL", nil) || ENV.fetch("DATABASE_URL", nil)
      @default_queue      = "default"
      @concurrency        = 5
      @batch_size         = 5
      @poll_interval      = 1.0
      @heartbeat_interval = 30.0
      @shutdown_timeout   = 25.0
      @connection_class   = nil
      # 15 minutes, matching the Rust worker, and comfortably under the 1 hour
      # `expire_seconds` default so layer 1 always fires first.
      @job_timeout        = 900.0
    end
  end
end
