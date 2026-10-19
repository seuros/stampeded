# frozen_string_literal: true

require "test_helper"

class DaemonTest < Minitest::Test
  def setup
    @saved = ENV.fetch("STAMPEDED_INSTALL_DIR", nil)
  end

  def teardown
    ENV["STAMPEDED_INSTALL_DIR"] = @saved
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
