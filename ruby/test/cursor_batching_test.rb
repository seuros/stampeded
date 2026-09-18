# frozen_string_literal: true

require "test_helper"

# Opt-in cursor batching.
#
# A checkpoint costs a round trip -- measured at ~1.7ms against a local
# database, so ~600/second -- which makes per-row checkpointing cost more than
# the work it is recording. `every:` trades durability for throughput, and the
# trade is written at the call site rather than hidden in the framework.
class CursorBatchingTest < Minitest::Test
  include StampededTest::Isolation

  # A context stub: Steps only needs #checkpoint to build itself, and we
  # replace #persist! so the test counts writes instead of doing them.
  def steps_for(writes)
    ctx = Object.new
    ctx.define_singleton_method(:checkpoint) { nil }
    state = Stampeded::Steps.new(ctx)
    state.define_singleton_method(:persist!) { writes << 1 }
    state
  end

  def test_advance_writes_every_time_by_default
    writes = []
    steps = steps_for(writes)

    steps.step("rows", cursor: 0) { |c| 10.times { |i| c.advance!(i) } }

    # 10 advances + the step-completion persist.
    assert_equal 11, writes.size
  end

  def test_every_batches_writes
    writes = []
    steps = steps_for(writes)

    steps.step("rows", cursor: 0) { |c| 100.times { |i| c.advance!(i, every: 25) } }

    # 100/25 = 4 writes, plus the step-completion persist.
    assert_equal 5, writes.size
  end

  # The value must be correct in memory even when the write is deferred --
  # otherwise a resumed job reads a cursor that never matched reality.
  def test_the_cursor_value_is_current_even_when_buffered
    steps = steps_for([])
    seen = nil

    steps.step("rows", cursor: 0) do |c|
      10.times { |i| c.advance!(i, every: 100) }
      seen = c.value
    end

    assert_equal 9, seen
  end

  def test_flush_writes_a_partial_batch
    writes = []
    steps = steps_for(writes)

    steps.step("rows", cursor: 0) do |c|
      3.times { |i| c.advance!(i, every: 100) }

      assert_equal 0, writes.size, "nothing should have been written yet"
      c.flush!
      assert_equal 1, writes.size, "flush! must write the partial batch"
      c.flush!
      assert_equal 1, writes.size, "flush! must be a no-op when nothing is pending"
    end
  end

  # A buffered value that never reached the database must NOT be written while
  # unwinding: it describes work that may not have committed, and persisting it
  # would make the retry skip that work instead of redoing it.
  def test_a_raising_step_does_not_flush_its_buffer
    writes = []
    steps = steps_for(writes)

    assert_raises(RuntimeError) do
      steps.step("rows", cursor: 0) do |c|
        5.times { |i| c.advance!(i, every: 100) }
        raise "boom"
      end
    end

    assert_equal 0, writes.size, "a pending cursor must not be written during unwinding"
  end
end
