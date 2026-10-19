# frozen_string_literal: true

module Stampeded
  Error = Class.new(StandardError)

  class RetryConflict < Error
    def initialize(job_class)
      super(<<~MSG)
        #{job_class} declares `retry_on`; Stampeded owns retries:

          class #{job_class} < ApplicationJob
            stampeded max_attempts: 5, backoff_base_ms: 1_000, backoff_max_ms: 300_000
          end

        For a specific delay: raise Stampeded::RetryIn.new(seconds). `discard_on` is fine.
      MSG
    end
  end

  class PortableArgumentsError < Error
    def initialize(job_class, task)
      super(<<~MSG)
        #{job_class} is task #{task.inspect}: pass keyword arguments with
        plain JSON values (strings, numbers, booleans, nil, arrays, hashes),
        ids rather than records, ISO 8601 strings rather than times:

          #{job_class}.perform_later(document_id: document.id)
      MSG
    end
  end

  class DaemonNotFound < Error; end
  class LibraryNotFound < Error; end
  class SchemaMissing < Error
    def initialize(cause = nil)
      super(<<~MSG)
        The Stampeded schema is not installed in this database.

        In Rails:

            bin/rails stampeded:install

        Elsewhere:

            Stampeded::Schema.install!

        Postgres 18 or newer is required.
        #{"\nUnderlying error: #{cause.message.lines.first&.strip}" if cause}
      MSG
    end
  end

  class UnknownTask < Error; end

  class Requeue < Exception; end
  class RetryIn < Error
    attr_reader :seconds

    def initialize(seconds, message = nil)
      @seconds = Float(seconds)
      raise ArgumentError, "RetryIn needs a non-negative delay, got #{seconds.inspect}" if @seconds.negative?

      super(message || "retry in #{@seconds}s")
    end

    def delay_ms = (@seconds * 1000).round
  end
end
