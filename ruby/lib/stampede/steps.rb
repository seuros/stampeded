# frozen_string_literal: true

require "json"
require "set"

module Stampede
  # Named steps within a job, fast-forwarded on resume.
  #
  # River's model (`resumable.go`), which is the prior art and better than a
  # single opaque cursor: a job declares its phases by name, and on resume the
  # ones already recorded complete are SKIPPED rather than re-run.
  #
  #   stampede_step("charge") { Stripe.charge(amount) }   # never charged twice
  #   stampede_step("email")  { Mailer.receipt.deliver }
  #
  # ONE DEPARTURE FROM RIVER, and it is the difference between this being
  # useful and being a footgun. River skips a completed step and returns
  # nothing, so the obvious usage silently breaks on resume:
  #
  #   data = stampede_step("download") { fetch(url) }   # nil on resume!
  #
  # So a step's return value is PERSISTED (when it is small and JSON-safe) and
  # handed back when the step is skipped. A large result raises rather than
  # being silently dropped, because the alternative is a job that works until
  # the first crash and then behaves differently.
  #
  # River also tracks only the LAST completed step, relying on a fixed linear
  # order. This tracks a SET, which behaves identically for the common case and
  # does the right thing when a step is conditional.
  class Steps
    VERSION = 1

    # Above this, a step result is refused rather than stored. The row cap is
    # 64 KB for everything; one step must not be able to eat it.
    MAX_VALUE_BYTES = 8 * 1024

    class DuplicateStep < Error; end
    class ResultTooLarge < Error; end

    def initialize(context)
      @ctx = context
      state = context.checkpoint || {}
      @completed = Set.new(state["completed"] || [])
      @values    = state["values"] || {}
      @cursors   = state["cursors"] || {}
      @seen      = Set.new
    end

    # Run a step once, ever.
    #
    # Returns the block's value, or the value it returned the first time if
    # this attempt is resuming past it.
    def step(name, cursor: nil, &block)
      name = name.to_s
      guard_unique!(name)

      return @values[name] if @completed.include?(name)

      result = cursor.nil? ? block.call : run_cursor_step(name, cursor, &block)

      @completed << name
      @cursors.delete(name)
      store_value(name, result)
      persist!
      result
    end

    # Has this step already run in an earlier attempt?
    def completed?(name) = @completed.include?(name.to_s)

    def completed_steps = @completed.to_a

    private

    def run_cursor_step(name, initial, &block)
      handle = Cursor.new(@cursors.fetch(name, initial), self, name)
      block.call(handle)
    end

    # Called by Cursor#advance!.
    def advance(name, value)
      @cursors[name] = value
      persist!
    end

    def store_value(name, result)
      # nil is the overwhelmingly common case -- a step run for its side
      # effects -- and storing it would bloat the state for nothing.
      return if result.nil?

      encoded = JSON.generate(result)
      if encoded.bytesize > MAX_VALUE_BYTES
        raise ResultTooLarge, <<~MSG
          Step #{name.inspect} returned #{encoded.bytesize} bytes; the limit is #{MAX_VALUE_BYTES}.

          Step results are stored so that a resumed job can skip the step and
          still see what it produced. That only works for small values -- an id,
          a path, a token. Write the large thing somewhere durable and return a
          reference to it:

            path = stampede_step("download") { save_to_disk(fetch(url)) }
        MSG
      end
      @values[name] = JSON.parse(encoded) # normalise symbols to strings
    rescue JSON::GeneratorError => e
      raise ResultTooLarge, "step #{name.inspect} returned something not JSON-serialisable: #{e.message}"
    end

    def guard_unique!(name)
      # Two steps with the same name would make "have I done this?" ambiguous
      # and the fast-forward silently wrong. River detects this too.
      raise DuplicateStep, "step #{name.inspect} is declared twice in this job" unless @seen.add?(name)
    end

    def persist!
      @ctx.checkpoint!({
        "v" => VERSION,
        "completed" => @completed.to_a,
        "values" => @values,
        "cursors" => @cursors
      })
    end

    # Position within a step, for work that is too big for one transaction.
    class Cursor
      attr_reader :value

      # `cursor` reads better at the call site than `value`:
      #   step.cursor...total
      alias cursor value

      def initialize(value, steps, name)
        @value = value
        @steps = steps
        @name = name
      end

      # Record progress within this step.
      #
      # COMMIT YOUR WORK FIRST, or pass the connection so both land together --
      # see JobContext#checkpoint!.
      def advance!(value)
        @value = value
        @steps.send(:advance, @name, value)
        value
      end

      def to_s = @value.to_s
    end
  end
end
