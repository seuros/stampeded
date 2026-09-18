# frozen_string_literal: true

require "minitest/autorun"
require "tmpdir"
require "open3"

# Boots a real Rails::Application IN A SUBPROCESS.
#
# The subprocess is not ceremony. These are LOAD-ORDER bugs, and this file's
# own test_helper requires the adapter by hand -- so a test running in the
# normal suite has the constant loaded already and cannot observe whether the
# railtie loads it. The first version of this test passed happily with the fix
# deleted, which is worse than having no test.
#
# The bug it exists for: `config.active_job.queue_adapter = :stampede` goes
# through ActiveJob::QueueAdapters.lookup, a bare `const_get`. Autoload covers
# the built-in adapters only and nothing in that path requires anything, so a
# real app died at boot with "uninitialized constant
# ActiveJob::QueueAdapters::StampedeAdapter" -- naming a class sitting right
# there in the gem.
class RailtieTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)

  # Run `script` in a fresh ruby, with only the gem on the load path.
  def boot(script)
    Dir.mktmpdir do |root|
      prelude = <<~RUBY
        require "rails"
        require "rake"
        require "active_job/railtie"
        require "stampede"          # railtie only; NOTHING required by hand

        class BootApp < Rails::Application
          config.eager_load = false
          config.logger = Logger.new(IO::NULL)
          config.root = #{root.inspect}
          config.active_job.queue_adapter = :stampede
        end
        BootApp.initialize!
      RUBY

      # Inherit the parent's load path so the subprocess sees the same gems,
      # but NOT its required files -- that is the whole point.
      includes = ($LOAD_PATH + [LIB]).uniq.flat_map { |p| ["-I", p] }

      out, err, status = Open3.capture3(
        RbConfig.ruby, *includes, "-e", "#{prelude}\n#{script}"
      )
      [out.strip, err.strip, status.success?]
    end
  end

  def test_a_real_app_boots_with_the_adapter_configured_by_name
    out, err, ok = boot('puts ActiveJob::Base.queue_adapter.class.name')

    assert ok, "Rails failed to boot:\n#{err}"
    assert_equal "ActiveJob::QueueAdapters::StampedeAdapter", out
  end

  def test_the_adapter_is_a_proper_abstract_adapter_subclass
    out, err, ok = boot(<<~RUBY)
      puts ActiveJob::Base.queue_adapter.is_a?(ActiveJob::QueueAdapters::AbstractAdapter)
    RUBY

    assert ok, err
    assert_equal "true", out
  end

  def test_the_dsl_is_mixed_into_job_classes
    out, err, ok = boot(<<~RUBY)
      puts ActiveJob::Base.respond_to?(:stampede) && ActiveJob::Base.respond_to?(:stampede_options)
    RUBY

    assert ok, err
    assert_equal "true", out
  end

  def test_retry_on_is_guarded_inside_a_real_app
    out, err, ok = boot(<<~RUBY)
      begin
        Class.new(ActiveJob::Base) do
          def self.name = "GuardedJob"
          retry_on StandardError
        end
        puts "NOT GUARDED"
      rescue Stampede::RetryConflict => e
        puts(e.message.include?("max_attempts") ? "guarded" : "guarded but unhelpful")
      end
    RUBY

    assert ok, err
    assert_equal "guarded", out
  end

  def test_the_rake_tasks_are_registered_by_the_railtie
    out, err, ok = boot(<<~RUBY)
      BootApp.load_tasks
      puts %w[stampede:install stampede:work stampede:daemon]
             .select { |t| Rake::Task.task_defined?(t) }.join(",")
    RUBY

    assert ok, err
    assert_equal "stampede:install,stampede:work,stampede:daemon", out
  end
end
