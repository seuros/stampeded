# frozen_string_literal: true

require "json"
require "set"

module Stampeded
  class Steps
    class DuplicateStep < Error; end
    class ResultTooLarge < Error; end

    def initialize(context)
      require "stampeded/native"
      @ctx = context
      @record = Native::Steps.new(context.checkpoint)
      @seen = Set.new
    end

    def step(name, cursor: nil, &block)
      name = name.to_s
      raise DuplicateStep, "step #{name.inspect} is declared twice in this job" unless @seen.add?(name)
      return @record.value(name) if @record.completed?(name)

      result = cursor.nil? ? block.call : run_cursor_step(name, cursor, &block)
      complete(name, result)
      persist!
      result
    end

    def completed?(name) = @record.completed?(name)

    def completed_steps = @record.to_h["completed"]

    private

    def run_cursor_step(name, initial, &block)
      saved = @record.cursor(name)
      block.call(Cursor.new(saved.nil? ? initial : saved, self, name))
    end

    def advance(name, value)
      @record.set_cursor(name, JSON.generate(value))
      persist!
    end

    def complete(name, result)
      json =
        begin
          result.nil? ? nil : JSON.generate(result)
        rescue JSON::GeneratorError => e
          raise ResultTooLarge, "step #{name.inspect} returned something not JSON-serialisable: #{e.message}"
        end
      @record.complete(name, json)
    rescue ResultTooLarge
      raise
    rescue Error => e
      raise ResultTooLarge, e.message
    end

    def persist! = @ctx.checkpoint!(@record.to_h)

    class Cursor
      attr_reader :value

      alias cursor value

      def initialize(value, steps, name)
        @value = value
        @steps = steps
        @name = name
        @since_write = 0
      end

      def advance!(value, every: 1)
        @value = value
        @since_write += 1
        return value if every > 1 && @since_write < every

        @since_write = 0
        @steps.send(:advance, @name, value)
        value
      end

      def flush!
        return @value if @since_write.zero?

        @since_write = 0
        @steps.send(:advance, @name, @value)
        @value
      end

      def to_s = @value.to_s
    end
  end
end
