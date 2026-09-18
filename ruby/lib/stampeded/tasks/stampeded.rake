# frozen_string_literal: true

namespace :stampeded do
  desc "Install the Stampeded schema (Postgres 18+)"
  task install: :environment do
    require "stampeded"
    Stampeded::Schema.install!
    puts "schema installed"
  end

  desc "Run an ActiveJob worker (QUEUE=default CONCURRENCY=5)"
  task work: :environment do
    require "stampeded/active_job_worker"

    worker = Stampeded::ActiveJobWorker.new(
      queue: ENV.fetch("QUEUE", Stampeded.config.default_queue),
      concurrency: ENV.fetch("CONCURRENCY", Stampeded.config.concurrency).to_i
    )
    puts "stampeded worker: queue=#{worker.queue} pid=#{Process.pid}"
    worker.run
  end

  desc "Run the daemon (scheduler, reaper, pruner, dashboard). Not the worker."
  task daemon: :environment do
    require "stampeded/daemon"
    # The binary takes a URL, so build one from whatever Connection resolved --
    # which inside Rails is the app's own database, not an env var.
    args = ["--database-url", Stampeded.database_url_for_daemon, "--no-worker"]
    args += ENV["STAMPEDED_ARGS"].to_s.split
    Stampeded::Daemon.exec!(*args)
  end
end
