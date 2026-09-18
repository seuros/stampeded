# frozen_string_literal: true

module Stampede
  class Configuration
    # Where the jobs live. Defaults to ActiveRecord's connection when Rails is
    # present, so enqueueing participates in the caller's transaction.
    attr_accessor :database_url

    # Default queue for `Stampede.enqueue` and for ActiveJob jobs that do not
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

    def initialize
      @database_url       = ENV.fetch("STAMPEDE_DATABASE_URL", nil) || ENV.fetch("DATABASE_URL", nil)
      @default_queue      = "default"
      @concurrency        = 5
      @batch_size         = 5
      @poll_interval      = 1.0
      @heartbeat_interval = 30.0
      @shutdown_timeout   = 25.0
    end
  end
end
