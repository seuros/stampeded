# frozen_string_literal: true

module Stampeded
  # Locates and launches the precompiled daemon.
  #
  # The daemon runs the roles that do NOT need to know what a job means:
  # scheduler, reaper, pruner, expirer, health checks, HTTP API, dashboard.
  # They operate on rows, so they are language-agnostic. The one role that has
  # to execute a job body stays in Ruby -- see plans/16 §2, and note that the
  # daemon has always had `--no-worker` for exactly this split.
  #
  # Binary resolution follows tailwindcss-ruby: exe/<platform>/<binary>, with
  # an install-dir escape hatch so a platform we do not ship, or a locally
  # built debug binary, has a supported way in.
  #
  # The binary lives in THIS gem. It used to be a separate `stampeded-daemon`
  # gem; one package is simpler to publish and version, and the binary is a
  # few MB.
  module Daemon
    BINARY = "stampeded"

    module_function

    def platform
      %i[cpu os].map { |m| Gem::Platform.local.public_send(m) }.join("-")
    end

    def executable
      if (dir = ENV.fetch("STAMPEDED_INSTALL_DIR", nil))
        path = File.expand_path(File.join(dir, BINARY))
        raise DaemonNotFound, "STAMPEDED_INSTALL_DIR is set to #{dir}, but #{path} is not executable" \
          unless File.executable?(path)

        return path
      end

      path = bundled_path
      return path if path && File.executable?(path)

      raise DaemonNotFound, <<~MSG
        Could not find the stampeded daemon for #{platform}.

        The gem ships a binary for each platform it is built for, so this means
        there is no build for #{platform} -- you have the platformless gem.
        Build the daemon and point at it:

          cargo build --release
          export STAMPEDED_INSTALL_DIR=/path/to/stampeded/target/release

        The daemon is OPTIONAL. Enqueueing and running jobs work without it --
        you lose the scheduler, the reaper, the pruner, the dashboard and the
        HTTP API, all of which operate on rows rather than on job bodies.
      MSG
    end

    # Relative to this file rather than via Gem::Specification.
    #
    # __dir__ is <root>/lib/stampeded, so ../.. is the gem root in an installed
    # gem and the `ruby/` directory in a checkout -- one expression that works
    # for both. Looking the gem up by name only worked installed, which is the
    # case nobody tests.
    # libexec, not exe: exe/ holds the Ruby launcher that RubyGems installs as
    # the `stampeded` command, and the real binary must not collide with it.
    # No platform subdirectory either -- a platform gem only ever contains its
    # own binary, so the extra level was vestigial. Matches guardship.
    def bundled_path
      File.expand_path(File.join("../..", "libexec", BINARY), __dir__)
    end

    def available?
      !executable.nil?
    rescue DaemonNotFound
      false
    end

    # Replace this process with the daemon.
    #
    # exec rather than spawn: under systemd, Docker or foreman the supervisor
    # should be watching the daemon itself, not a Ruby parent that would
    # swallow its exit status and its signals.
    def exec!(*args)
      Kernel.exec(executable, *args)
    end
  end
end
