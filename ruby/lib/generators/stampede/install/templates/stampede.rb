# frozen_string_literal: true

Stampede.configure do |config|
  # Worker pool size.
  #
  # THREADS, not processes. Rails jobs are overwhelmingly IO-bound and the GVL
  # is released for database calls, HTTP and file IO. CPU-bound jobs will NOT
  # scale on this pool -- run more processes with a smaller pool each, which is
  # the same advice Sidekiq gives.
  config.concurrency = 5

  # Claimed per round trip.
  config.batch_size = 5

  # Poll floor. Never disabled: with NOTIFY wired up a job starts in
  # milliseconds, but a redundant poll costs one empty query and a missed
  # wakeup costs a stuck job.
  config.poll_interval = 1.0

  # Must stay comfortably under the daemon's dead_after (default 5 minutes), or
  # healthy workers get reaped mid-job.
  config.heartbeat_interval = 30.0

  # How long in-flight jobs get to finish on SIGTERM. Jobs that outlive it are
  # reclaimed by the reaper and charged an interruption, which is correct --
  # from the queue's point of view the process died on them.
  config.shutdown_timeout = 25.0

  # NOT set here on purpose: database_url.
  #
  # Inside Rails the app's own ActiveRecord configuration is authoritative, and
  # copying a URL here would introduce a second source of truth that silently
  # wins when the two disagree. That matters more than it sounds: the enqueue is
  # an INSERT on your connection, so jobs landing in a different database than
  # your data would quietly stop being transactional.
end
