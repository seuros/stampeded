# frozen_string_literal: true

require "rails/railtie"

module Stampede
  class Railtie < Rails::Railtie
    rake_tasks do
      load File.expand_path("tasks/stampede.rake", __dir__)
    end

    initializer "stampede.active_job" do
      ActiveSupport.on_load(:active_job) do
        require "stampede/active_job_ext"
        include Stampede::ActiveJobExt
      end
    end

    initializer "stampede.config" do |app|
      # Default to the app's own database. The whole transactional-enqueue
      # property depends on jobs living in the same database as the business
      # data -- a separate queue database would put the INSERT in a different
      # transaction and give away the one thing Redis cannot do.
      Stampede.config.database_url ||= app.config.database_configuration
                                          .dig(Rails.env, "url")
    end
  end
end
