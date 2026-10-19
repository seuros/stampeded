# frozen_string_literal: true

require "test_helper"
require "open3"

class DaemonTest < Minitest::Test
  LIB = File.expand_path("../lib", __dir__)
  EXE = File.expand_path("../exe/stampeded", __dir__)

  def setup
    @saved = ENV.fetch("STAMPEDED_INSTALL_DIR", nil)
  end

  def teardown
    ENV["STAMPEDED_INSTALL_DIR"] = @saved
  end

  def against_a_fake_daemon(env, *command)
    Dir.mktmpdir do |dir|
      bin = File.join(dir, "stampeded")
      File.write(bin, <<~'SH')
        #!/bin/sh
        printf '%s\n' "$@" > "$0.argv"
        printf '%s' "${STAMPEDED_DATABASE_URL-unset}" > "$0.env"
      SH
      File.chmod(0o755, bin)

      includes = ($LOAD_PATH + [LIB]).uniq.flat_map { ["-I", it] }
      clean = { "STAMPEDED_INSTALL_DIR" => dir, "STAMPEDED_DATABASE_URL" => nil, "DATABASE_URL" => nil }
      _out, err, status = Open3.capture3(clean.merge(env), RbConfig.ruby, *includes, *command)

      assert status.success?, err
      yield File.read("#{bin}.argv").lines(chomp: true), File.read("#{bin}.env")
    end
  end

  def test_the_daemon_task_hands_the_fake_daemon_its_url_in_the_environment_never_in_argv
    url = "postgres://app:s3cret-pw@db.internal:5432/prod"
    task = <<~RUBY
      require "rake"
      require "stampeded"
      Stampeded.config.database_url = #{url.inspect}
      Rake::Task.define_task(:environment)
      load #{File.join(LIB, "stampeded/tasks/stampeded.rake").inspect}
      Rake::Task["stampeded:daemon"].invoke
    RUBY

    against_a_fake_daemon({ "STAMPEDED_ARGS" => "--http 127.0.0.1:7878" }, "-e", task) do |argv, env_url|
      assert_equal ["--no-worker", "--http", "127.0.0.1:7878"], argv
      refute argv.any? { it.include?("s3cret-pw") },
             "argv is world-readable through ps and /proc: the password must not be in it"
      assert_equal url, env_url
    end
  end

  def test_the_executable_passes_its_arguments_through_to_the_fake_daemon_untouched
    against_a_fake_daemon({}, EXE, "--queue", "mail", "--no-maintenance") do |argv, env_url|
      assert_equal ["--queue", "mail", "--no-maintenance"], argv
      assert_equal "unset", env_url, "the executable adds nothing to the environment"
    end
  end

  def test_the_install_dir_escape_hatch_finds_a_locally_built_binary
    Dir.mktmpdir do |dir|
      bin = File.join(dir, "stampeded")
      File.write(bin, "#!/bin/sh\necho hi\n")
      File.chmod(0o755, bin)

      ENV["STAMPEDED_INSTALL_DIR"] = dir
      assert_equal bin, Stampeded::Daemon.executable
    end
  end

  def test_a_missing_binary_names_the_platform_and_the_way_out
    ENV["STAMPEDED_INSTALL_DIR"] = nil
    Stampeded::Daemon.stub(:bundled_path, nil) do
      error = assert_raises(Stampeded::DaemonNotFound) { Stampeded::Daemon.executable }

      assert_match(/#{Regexp.escape(Stampeded::Daemon.platform)}/, error.message)
      assert_match(/no build for/, error.message)
      refute_match(/gem "stampeded-daemon"/, error.message,
                   "there is no separate daemon gem any more")
      assert_match(/cargo build --release/, error.message)
      assert_match(/STAMPEDED_INSTALL_DIR/, error.message,
                   "must say how to point at the binary once built")
      assert_match(/OPTIONAL/, error.message,
                   "must say the queue works without it, or people will think it is required")
    end
  end

  def test_a_bad_install_dir_is_reported_rather_than_silently_ignored
    ENV["STAMPEDED_INSTALL_DIR"] = "/nonexistent/path"
    error = assert_raises(Stampeded::DaemonNotFound) { Stampeded::Daemon.executable }
    assert_match(%r{/nonexistent/path}, error.message)
  end

  def test_platform_looks_like_a_rubygems_platform
    assert_match(/\A[\w.]+-\w+\z/, Stampeded::Daemon.platform)
  end
end
