# frozen_string_literal: true

require "test_helper"

# The gem is a launcher for a precompiled daemon, so "does it install a working
# command" is a correctness property, not packaging trivia.
#
# It did not. The binary shipped inside the gem but no executable was DECLARED,
# so RubyGems installed no command at all -- verified by installing into an
# empty GEM_HOME and finding bin/ empty. The daemon could only be started
# through a Rails rake task, which is backwards for a process whose whole point
# is that it does not need the application.
#
# Shape follows guardship/mothership: exe/<name> is a Ruby shim that execs
# libexec/<name>.
class PackagingTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)

  def spec
    @spec ||= Gem::Specification.load(File.join(ROOT, "stampeded.gemspec"))
  end

  def test_the_gem_installs_a_stampeded_command
    assert_equal ["stampeded"], spec.executables
    assert_equal "exe", spec.bindir
  end

  def test_the_launcher_ships_and_is_executable
    shim = File.join(ROOT, "exe", "stampeded")

    assert File.file?(shim), "exe/stampeded is missing; the gem declares it as its executable"
    assert File.executable?(shim), "exe/stampeded is not executable"
    assert_includes spec.files, "exe/stampeded"
  end

  def test_the_launcher_is_valid_ruby_and_defers_to_daemon
    shim = File.read(File.join(ROOT, "exe", "stampeded"))

    # Syntax-check without running it: it would exec a daemon.
    assert RubyVM::InstructionSequence.compile(shim), "exe/stampeded does not parse"

    # It must go through Daemon so STAMPEDED_INSTALL_DIR keeps working and only
    # one place knows where the binary lives.
    assert_match(/Stampeded::Daemon\.exec!/, shim)
    refute_match(%r{libexec/stampeded["']}, shim,
                 "the launcher hardcodes the binary path instead of asking Daemon")
  end

  def test_the_binary_is_resolved_from_libexec_not_the_bindir
    # If the binary sat in exe/ it would collide with the launcher RubyGems
    # installs under the same name.
    assert_match(%r{/libexec/stampeded\z}, Stampeded::Daemon.bundled_path)
  end

  def test_a_platformless_gem_ships_no_binary
    ENV.delete("STAMPEDED_GEM_PLATFORM")
    generic = Gem::Specification.load(File.join(ROOT, "stampeded.gemspec"))

    refute_includes generic.files, "libexec/stampeded",
                    "the platformless gem must not claim a binary it has not built"
    assert_equal ["stampeded"], generic.executables,
                 "the stub still needs the command, so it can explain itself"
  end

  def test_the_missing_binary_message_is_actionable
    Stampeded::Daemon.stub(:bundled_path, "/nonexistent/stampeded") do
      ENV.delete("STAMPEDED_INSTALL_DIR")
      err = assert_raises(Stampeded::DaemonNotFound) { Stampeded::Daemon.executable }

      assert_match(/STAMPEDED_INSTALL_DIR/, err.message, "the error must name the escape hatch")
      assert_match(/cargo build/, err.message, "the error must say how to get a binary")
    end
  end
end
