# frozen_string_literal: true

module Stampeded
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
        Could not find the stampeded daemon: there is no build for #{platform}.
        Build it and point at it:

          cargo build --release --bin stampeded
          export STAMPEDED_INSTALL_DIR=/path/to/stampeded/target/release

        The daemon is OPTIONAL: without it jobs still run, with no scheduler,
        reaper, pruner or dashboard.
      MSG
    end

    def bundled_path
      File.expand_path(File.join("../..", "libexec", BINARY), __dir__)
    end

    def available?
      !executable.nil?
    rescue DaemonNotFound
      false
    end

    def exec!(*args)
      Kernel.exec(executable, *args)
    end
  end
end
