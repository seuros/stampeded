# frozen_string_literal: true

module Stampeded
  class Configuration
    attr_accessor :database_url

    attr_accessor :default_queue

    attr_accessor :queues

    attr_accessor :except_queues

    attr_accessor :concurrency

    attr_accessor :batch_size

    attr_accessor :poll_interval

    attr_accessor :heartbeat_interval

    attr_accessor :shutdown_timeout

    attr_accessor :connection_class

    attr_accessor :job_timeout

    def initialize
      @database_url       = ENV.fetch("STAMPEDED_DATABASE_URL", nil) || ENV.fetch("DATABASE_URL", nil)
      @default_queue      = "default"
      @queues             = nil
      @except_queues      = nil
      @concurrency        = 5
      @batch_size         = 5
      @poll_interval      = 1.0
      @heartbeat_interval = 30.0
      @shutdown_timeout   = 25.0
      @connection_class   = nil
      @job_timeout        = 900.0
    end
  end
end
