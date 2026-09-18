# frozen_string_literal: true

module Stampede
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
  module Daemon
    GEM_NAME = "stampede-daemon"
    BINARY   = "stampede"

    module_function

    def platform
      %i[cpu os].map { |m| Gem::Platform.local.public_send(m) }.join("-")
    end

    def executable
      if (dir = ENV.fetch("STAMPEDE_INSTALL_DIR", nil))
        path = File.expand_path(File.join(dir, BINARY))
        raise DaemonNotFound, "STAMPEDE_INSTALL_DIR is set to #{dir}, but #{path} is not executable" \
          unless File.executable?(path)

        return path
      end

      path = bundled_path
      return path if path && File.executable?(path)

      raise DaemonNotFound, <<~MSG
        Could not find the stampede daemon for #{platform}.

        The daemon ships as a platform-specific gem. Add it:

          gem "stampede-daemon"

        If there is no gem for #{platform}, build it from source and point at it:

          cargo build --release
          export STAMPEDE_INSTALL_DIR=/path/to/stampede/target/release

        The daemon is OPTIONAL. Enqueueing and running jobs work without it --
        you lose the scheduler, the reaper, the pruner, the dashboard and the
        HTTP API, all of which operate on rows rather than on job bodies.
      MSG
    end

    def bundled_path
      spec = Gem::Specification.find_by_name(GEM_NAME)
      File.join(spec.gem_dir, "exe", platform, BINARY)
    rescue Gem::LoadError
      nil
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
