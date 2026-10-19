# frozen_string_literal: true

require "test_helper"

class CursorBatchingTest < Minitest::Test
  include StampededTest::Isolation

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

    assert_equal 11, writes.size
  end

  def test_every_batches_writes
    writes = []
    steps = steps_for(writes)

    steps.step("rows", cursor: 0) { |c| 100.times { |i| c.advance!(i, every: 25) } }

    assert_equal 5, writes.size
  end

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
