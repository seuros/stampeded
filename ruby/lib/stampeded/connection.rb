# frozen_string_literal: true

require "pg"
require "uri"

module Stampeded
  module Connection
    module_function

    def with(&block)
      if active_record?
        ar_class.connection.raw_connection.then(&block)
      else
        own(&block)
      end
    end

    def own
      conn = PG.connect(connection_params)
      begin
        yield conn
      ensure
        conn.close
      end
    end

    def connection_params
      explicit = Stampeded.config.database_url

      if active_record?
        cfg = ar_class.connection_db_config.configuration_hash
        return {
          host: cfg[:host] || "localhost",
          port: cfg[:port],
          dbname: cfg[:database],
          user: cfg[:username],
          password: cfg[:password],
          **cfg.slice(*TLS_KEYS)
        }.compact
      end

      explicit or raise Error, <<~MSG
        No database configured.

        Set Stampeded.config.database_url, or STAMPEDED_DATABASE_URL, or run
        inside a Rails app where ActiveRecord is configured.
      MSG
    end

    TLS_KEYS = %i[sslmode sslrootcert sslcert sslkey].freeze

    def url(params = connection_params)
      return params if params.is_a?(String)

      query = params.filter_map { |k, v| "#{k}=#{URI.encode_www_form_component(v.to_s)}" unless v.nil? }
      "postgres://?#{query.join("&")}"
    end

    def translating_errors
      yield
    rescue PG::UndefinedTable, PG::UndefinedFunction => e
      raise SchemaMissing, e if e.message.include?("stampeded_")

      raise
    end

    def ar_class
      name = Stampeded.config.connection_class
      return ActiveRecord::Base if name.nil?

      Object.const_get(name.to_s)
    rescue NameError
      raise Error, "Stampeded.config.connection_class is #{name.inspect}, which does not exist"
    end

    def active_record?
      defined?(ActiveRecord::Base) && ActiveRecord::Base.respond_to?(:connection_db_config)
    rescue StandardError
      false
    end
  end
end
