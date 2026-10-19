# frozen_string_literal: true

Stampeded.configure do |config|
  config.concurrency = 5

  config.batch_size = 5

  config.poll_interval = 1.0

  config.heartbeat_interval = 30.0

  config.shutdown_timeout = 25.0
end
