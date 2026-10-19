# frozen_string_literal: true

require "minitest/autorun"
require "tmpdir"
require "open3"

class RailtieTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)

  def boot(script)
    Dir.mktmpdir do |root|
      prelude = <<~RUBY
        require "rails"
        require "rake"
        require "active_job/railtie"
        require "stampeded"          # railtie only; NOTHING required by hand

        class BootApp < Rails::Application
          config.eager_load = false
          config.logger = Logger.new(IO::NULL)
          config.root = #{root.inspect}
          config.active_job.queue_adapter = :stampeded
        end
        BootApp.initialize!
      RUBY

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
    assert_equal "ActiveJob::QueueAdapters::StampededAdapter", out
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
      puts ActiveJob::Base.respond_to?(:stampeded) && ActiveJob::Base.respond_to?(:stampeded_options)
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
      rescue Stampeded::RetryConflict => e
        puts(e.message.include?("max_attempts") ? "guarded" : "guarded but unhelpful")
      end
    RUBY

    assert ok, err
    assert_equal "guarded", out
  end

  def test_the_rake_tasks_are_registered_by_the_railtie
    out, err, ok = boot(<<~RUBY)
      BootApp.load_tasks
      puts %w[stampeded:install stampeded:work stampeded:daemon]
             .select { |t| Rake::Task.task_defined?(t) }.join(",")
    RUBY

    assert ok, err
    assert_equal "stampeded:install,stampeded:work,stampeded:daemon", out
  end

  def test_the_activejob_worker_is_available_without_the_rake_task
    out, err, ok = boot('puts Stampeded::ActiveJobWorker.name')

    assert ok, "booting failed: #{err}"
    assert_equal "Stampeded::ActiveJobWorker", out.strip
  end
end
