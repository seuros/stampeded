# frozen_string_literal: true

require "rails/railtie"

module Stampeded
  class Railtie < Rails::Railtie
    rake_tasks do
      load File.expand_path("tasks/stampeded.rake", __dir__)
    end

    initializer "stampeded.active_job", before: "active_job.set_configs" do
      ActiveSupport.on_load(:active_job) do
        require "active_job/queue_adapters/stampeded_adapter"
        require "stampeded/active_job_ext"
        require "stampeded/active_job_worker"
        include Stampeded::ActiveJobExt
      end
    end

    initializer "stampeded.config" do
    end
  end
end
