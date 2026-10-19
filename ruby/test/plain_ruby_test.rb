# frozen_string_literal: true

require "test_helper"

class PlainRubyTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)

  def ruby(code)
    out = IO.popen([RbConfig.ruby, "-I", LIB, "-e", code], err: %i[child out], &:read)
    [out, $?.success?]
  end

  def test_the_gem_does_not_rely_on_rails_to_define_timeout
    out, ok = ruby('require "stampeded"; puts defined?(Timeout) ? "yes" : "no"')

    assert ok, "requiring stampeded on its own failed: #{out}"
    assert_equal "yes", out.strip,
                 "worker.rb references Timeout::Error but nothing requires timeout; " \
                 "outside Rails that is a NameError while reporting a job failure"
  end

  def test_failure_classification_works_without_rails
    out, ok = ruby(<<~RUBY)
      require "stampeded"
      w = Stampeded::Worker.new(queue: "default")
      puts w.send(:failure_kind_for, ArgumentError.new("boom"))
    RUBY

    assert ok, "classifying an ordinary error failed outside Rails: #{out}"
    assert_equal "raised", out.strip
  end
end
