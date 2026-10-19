# frozen_string_literal: true

require "rails/generators"

module Stampeded
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      class_option :queue, type: :string, default: "default",
                           desc: "Queue to provision a partition for"

      def create_initializer
        template "stampeded.rb", "config/initializers/stampeded.rb"
      end

      def install_schema
        if Stampeded::Schema.installed?
          say_status :schema, "already installed, skipping", :blue
        else
          say_status :schema, "applying stampeded schema (Postgres 18+)"
          Stampeded::Schema.install!
        end

        Stampeded.create_queue(options[:queue])
      rescue StandardError => e
        say_status :error, e.message, :red
        say <<~MSG
          The schema was not applied. Check that the database exists and runs
          Postgres 18 or newer, then: bin/rails stampeded:install
        MSG
      end

      def show_next_steps
        say <<~MSG

          Stampeded installed.

            config.active_job.queue_adapter = :stampeded    # in config/application.rb

            bin/rails stampeded:work      # runs your jobs
            bin/rails stampeded:daemon    # scheduler, reaper, pruner, dashboard
        MSG
      end
    end
  end
end
