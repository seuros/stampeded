# frozen_string_literal: true

require_relative "continuable_ext"

module Stampeded
  module ActiveJobExt
    def self.included(base)
      base.extend(ClassMethods)
    end

    def self.installed_gem?(file)
      Gem.path.any? { |dir| file.start_with?(File.join(dir, "")) }
    end

    def stampeded_checkpoint
      Stampeded::Current.context&.checkpoint
    end

    def stampeded_checkpoint!(state = nil, conn: nil, **rest)
      ctx = Stampeded::Current.context
      raise Stampeded::Error, "no Stampeded job context: is this running under a Stampeded worker?" if ctx.nil?

      ctx.checkpoint!(state || rest, conn: conn)
    end

    def stampeded_step(name, cursor: nil, &)
      ctx = Stampeded::Current.context
      raise Stampeded::Error, "no Stampeded job context: is this running under a Stampeded worker?" if ctx.nil?

      ctx.step(name, cursor: cursor, &)
    end

    def stampeded_resumed?
      !Stampeded::Current.context&.checkpoint.nil?
    end

    def stampeded_attempt
      Stampeded::Current.context&.attempt
    end

    module ClassMethods
      def stampeded_task = @stampeded_task

      def stampeded_options
        @stampeded_options ||= superclass.respond_to?(:stampeded_options) ? superclass.stampeded_options.dup : {}
      end

      def stampeded(**options)
        unknown = options.keys - %i[task max_attempts destination dead_letter_queue
                                    expire_seconds unique unique_period
                                    backoff_base_ms backoff_max_ms]
        raise ArgumentError, "unknown stampeded options: #{unknown.join(", ")}" if unknown.any?

        if (task = options.delete(:task))
          Stampeded::Envelope.register(task, self)
          @stampeded_task = task.to_s
        end

        base = options[:backoff_base_ms]
        if base && base.to_i > 32_767
          raise ArgumentError,
                "backoff_base_ms is at most 32767 (smallint on the row), got #{base}. " \
                "It is the FIRST retry delay, which doubles from there; the ceiling " \
                "you want is probably backoff_max_ms, which is an integer."
        end

        stampeded_options.merge!(options)
      end

      def retry_on(*, **)
        if stampeded_adapter?
          site = caller_locations(1, 1).first&.path.to_s
          raise Stampeded::RetryConflict, name || "This job" unless Stampeded::ActiveJobExt.installed_gem?(site)

          Kernel.warn "[stampeded] #{name} (#{site}) declares retry_on; its retries are not counted as attempts"
        end

        super
      end

      def stampeded_adapter?
        adapter = respond_to?(:queue_adapter) ? queue_adapter : nil
        adapter.class.name.to_s.include?("Stampeded")
      rescue StandardError
        false
      end
    end
  end
end
