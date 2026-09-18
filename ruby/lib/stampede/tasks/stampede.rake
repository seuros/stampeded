# frozen_string_literal: true

namespace :stampede do
  desc "Install the Stampede schema (Postgres 18+)"
  task install: :environment do
    require "stampede"
    Stampede::Schema.install!
    puts "schema installed"
  end

  desc "Run an ActiveJob worker (QUEUE=default CONCURRENCY=5)"
  task work: :environment do
    require "stampede/active_job_worker"

    worker = Stampede::ActiveJobWorker.new(
      queue: ENV.fetch("QUEUE", Stampede.config.default_queue),
      concurrency: ENV.fetch("CONCURRENCY", Stampede.config.concurrency).to_i
    )
    puts "stampede worker: queue=#{worker.queue} pid=#{Process.pid}"
    worker.run
  end

  desc "Run the daemon (scheduler, reaper, pruner, dashboard). Not the worker."
  task daemon: :environment do
    require "stampede/daemon"
    # The binary takes a URL, so build one from whatever Connection resolved --
    # which inside Rails is the app's own database, not an env var.
    args = ["--database-url", Stampede.database_url_for_daemon, "--no-worker"]
    args += ENV["STAMPEDE_ARGS"].to_s.split
    Stampede::Daemon.exec!(*args)
  end
end
