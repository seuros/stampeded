# frozen_string_literal: true

require_relative "stampeded/version"
require_relative "stampeded/errors"
require_relative "stampeded/configuration"
require_relative "stampeded/connection"
require "forwardable"
require_relative "stampeded/lifecycle"
require_relative "stampeded/job"
require_relative "stampeded/job_context"
require_relative "stampeded/current"
require_relative "stampeded/steps"
require_relative "stampeded/client"
require_relative "stampeded/envelope"
require_relative "stampeded/schema"
require_relative "stampeded/schedule"
require_relative "stampeded/queue_policy"
require_relative "stampeded/worker"
require_relative "stampeded/daemon"

module Stampeded
  autoload :ActiveJobWorker, File.expand_path("stampeded/active_job_worker", __dir__)
end

require_relative "stampeded/railtie" if defined?(Rails::Railtie)

module Stampeded
  def self.truthy?(value)
    value == true || value == "t" || value == "true"
  end

  class << self
    def config
      @config ||= Configuration.new
    end

    def configure
      yield config
    end

    attr_writer :stopping

    def stopping? = @stopping == true

    def enqueue(queue:, task:, **opts)
      Client.enqueue(queue: queue, task: task, **opts)
    end

    def database_url_for_daemon
      params = Connection.connection_params
      return params if params.is_a?(String)

      user = [params[:user], params[:password]].compact.join(":")
      host = [params[:host], params[:port]].compact.join(":")
      "postgres://#{user}#{"@" unless user.empty?}#{host}/#{params[:dbname]}"
    end

    def create_queue(name)
      Connection.with { |c| c.exec_params("SELECT stampeded_create_queue($1)", [name]) }
      name
    end

    def depth(queue)
      Connection.with do |c|
        c.exec_params(
          "SELECT state, n FROM stampeded_queue_counts WHERE queue = $1 AND n <> 0 ORDER BY state",
          [queue]
        ).each_with_object({}) { |r, h| h[r["state"]] = r["n"].to_i }
      end
    end
  end
end
