# frozen_string_literal: true

require "test_helper"

# What the gem looks like to someone who never loads Rails.
#
# Every other test in this suite runs in a process where test_helper has
# required active_job, and therefore ActiveSupport, and therefore `timeout`.
# That masks any dependency the gem has on a constant it never required: the
# first version of this test asserted `defined?(Timeout)` in-process and passed
# with the bug reverted, which is no test at all.
#
# So these boot a clean interpreter that requires ONLY stampede. The gem
# documents a plain-Ruby worker (`Worker#register`) as a supported way to use
# the queue without the framework, so this is a real configuration, not a
# contrived one.
class PlainRubyTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)

  def ruby(code)
    out = IO.popen([RbConfig.ruby, "-I", LIB, "-e", code], err: %i[child out], &:read)
    [out, $?.success?]
  end

  # worker.rb matches on Timeout::Error in failure_kind_for. That clause is
  # evaluated for ANY error that is not UnknownTask, so without the require it
  # raised NameError inside the rescue that was reporting the failure: the job
  # was never reported, sat in `executing` with a null last_error, and was
  # eventually reclaimed by the reaper as though the worker had died -- charging
  # an interruption for a clean application error.
  def test_the_gem_does_not_rely_on_rails_to_define_timeout
    out, ok = ruby('require "stampede"; puts defined?(Timeout) ? "yes" : "no"')

    assert ok, "requiring stampede on its own failed: #{out}"
    assert_equal "yes", out.strip,
                 "worker.rb references Timeout::Error but nothing requires timeout; " \
                 "outside Rails that is a NameError while reporting a job failure"
  end

  # The classifier itself, exercised the way the worker exercises it.
  def test_failure_classification_works_without_rails
    out, ok = ruby(<<~RUBY)
      require "stampede"
      w = Stampede::Worker.new(queue: "default")
      puts w.send(:failure_kind_for, ArgumentError.new("boom"))
    RUBY

    assert ok, "classifying an ordinary error failed outside Rails: #{out}"
    assert_equal "raised", out.strip
  end
end
