# frozen_string_literal: true

namespace :stampeded do
  desc "Install the Stampeded schema (Postgres 18+)"
  task install: :environment do
    require "stampeded"
    Stampeded::Schema.install!
    puts "schema installed"
  end

  desc "Run an ActiveJob worker (QUEUES=default,mail or QUEUES=* QUEUES_EXCEPT=dlq; CONCURRENCY=5)"
  task work: :environment do
    require "stampeded/active_job_worker"
    Rails.application.eager_load! unless Rails.application.config.eager_load

    worker = Stampeded::ActiveJobWorker.new(
      queues: ENV["QUEUES"] || ENV.fetch("QUEUE", nil),
      except: ENV.fetch("QUEUES_EXCEPT", nil),
      concurrency: ENV.fetch("CONCURRENCY", Stampeded.config.concurrency).to_i
    )
    puts "stampeded worker: queues=#{worker.all_queues? ? "*" : worker.queues.join(",")} pid=#{Process.pid}"
    worker.run
  end

  desc "Run the daemon (scheduler, reaper, pruner, dashboard). Not the worker."
  task daemon: :environment do
    require "stampeded/daemon"
    args = ["--no-worker", *ENV["STAMPEDED_ARGS"].to_s.split]
    Stampeded::Daemon.exec!(*args, env: { "STAMPEDED_DATABASE_URL" => Stampeded.database_url_for_daemon })
  end
end
