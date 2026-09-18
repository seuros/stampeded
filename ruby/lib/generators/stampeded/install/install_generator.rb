# frozen_string_literal: true

require "rails/generators"

module Stampeded
  module Generators
    # `bin/rails generate stampeded:install`
    #
    # Writes an initializer and applies the schema. Deliberately does NOT write
    # a Rails migration: the gem ships schema/*.sql verbatim and applies it, so
    # there is ONE definition of the schema. A gem-maintained migration would be
    # a second one, on its own release cycle, free to drift from what the Rust
    # daemon is built against (plans/16 §5).
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      class_option :queue, type: :string, default: "default",
                           desc: "Queue to provision a partition for"

      def create_initializer
        template "stampeded.rb", "config/initializers/stampeded.rb"
      end

      def install_schema
        # Idempotent, because running a generator twice is normal -- someone
        # re-runs it after editing the initializer, or it is in a setup script.
        # The schema uses bare CREATE TABLE, so applying it twice raises; a
        # scary error on a database that is already correctly set up is a worse
        # first impression than doing nothing.
        if Stampeded::Schema.installed?
          say_status :schema, "already installed, skipping", :blue
        else
          say_status :schema, "applying stampeded schema (Postgres 18+)"
          Stampeded::Schema.install!
        end

        # Always safe: stampeded_create_queue is a no-op for an existing queue.
        Stampeded.create_queue(options[:queue])
      rescue StandardError => e
        say_status :error, e.message, :red
        say <<~MSG
          The schema was not applied. Check that the database exists and is
          Postgres 18 or newer -- Stampeded uses uuidv7(), RETURNING OLD./NEW.
          and generated columns, with no compatibility shims.

          You can retry with: bin/rails stampeded:install
        MSG
      end

      def show_next_steps
        say <<~MSG

          Stampeded installed.

            config.active_job.queue_adapter = :stampeded    # in config/application.rb

            bin/rails stampeded:work      # runs your jobs
            bin/rails stampeded:daemon    # scheduler, reaper, pruner, dashboard

          Two processes, because the Rust daemon cannot execute an ActiveJob job
          -- that is a Ruby class. Everything else it does operates on rows.
        MSG
      end
    end
  end
end
