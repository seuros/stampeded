# frozen_string_literal: true

require "rails/railtie"

module Stampeded
  class Railtie < Rails::Railtie
    rake_tasks do
      load File.expand_path("tasks/stampeded.rake", __dir__)
    end

    initializer "stampeded.active_job", before: "active_job.set_configs" do
      ActiveSupport.on_load(:active_job) do
        # The adapter constant must EXIST before Rails resolves the name.
        #
        # `config.active_job.queue_adapter = :stampeded` ends up in
        # ActiveJob::QueueAdapters.lookup, which is a bare `const_get` --
        # autoload is registered for the built-in adapters only, and there is
        # no `require` anywhere in that path. A third-party adapter that is not
        # already loaded fails with an uninitialized-constant error naming a
        # class that is sitting right there in the gem.
        #
        # `before:` matters too: active_job.set_configs registers the on_load
        # hook that applies queue_adapter=, and hooks run in registration
        # order, so ours has to be registered first.
        require "active_job/queue_adapters/stampeded_adapter"
        require "stampeded/active_job_ext"
        include Stampeded::ActiveJobExt
      end
    end

    initializer "stampeded.config" do
      # Deliberately does NOT set database_url from ENV or from database.yml.
      #
      # Inside Rails, Connection derives everything from
      # ActiveRecord::Base.connection_db_config, which is the app's real
      # configuration. Copying a URL here would introduce a SECOND source of
      # truth that silently wins when the two disagree -- and they disagree
      # often, because DATABASE_URL is frequently set in a shell for unrelated
      # reasons while database.yml names something else entirely.
      #
      # The whole transactional-enqueue property depends on jobs living in the
      # same database as the business data, so there must be exactly one answer
      # to "which database".
    end
  end
end
