# frozen_string_literal: true

require "ffi"
require "json"

module Stampeded
  module Native
    extend FFI::Library

    OK    = 0
    NONE  = 1
    ERROR = -1

    LIBRARY = FFI.map_library_name("stampeded_ffi")

    def self.library_path
      if (dir = ENV.fetch("STAMPEDED_INSTALL_DIR", nil))
        path = File.expand_path(File.join(dir, LIBRARY))
        raise LibraryNotFound, "STAMPEDED_INSTALL_DIR is set to #{dir}, but #{path} does not exist" \
          unless File.file?(path)

        return path
      end

      path = File.expand_path(File.join("../..", "libexec", LIBRARY), __dir__)
      return path if File.file?(path)

      raise LibraryNotFound, <<~MSG
        Could not find #{LIBRARY} for #{Daemon.platform}. Build it and point at it:

          cargo build --release -p stampeded-ffi
          export STAMPEDED_INSTALL_DIR=/path/to/stampeded/target/release
      MSG
    end

    ffi_lib library_path

    class PullerOptions < FFI::Struct
      layout :queues, :pointer,
             :except_queues, :pointer,
             :max_leases, :uint32,
             :batch_size, :uint32,
             :poll_interval_ms, :uint32,
             :heartbeat_interval_ms, :uint32
    end

    attach_function :stampeded_version, [], :string
    attach_function :stampeded_last_error, [], :string
    attach_function :stampeded_log_init, [:string], :int32

    attach_function :stampeded_connect, [:string], :pointer, blocking: true
    attach_function :stampeded_client_free, [:pointer], :void, blocking: true

    attach_function :stampeded_puller_start, [:pointer, PullerOptions.by_ref], :pointer, blocking: true
    attach_function :stampeded_puller_worker_id, [:pointer], :string
    attach_function :stampeded_puller_next, %i[pointer uint32 pointer], :int32, blocking: true
    attach_function :stampeded_puller_stop, %i[pointer uint32], :int32, blocking: true
    attach_function :stampeded_puller_free, [:pointer], :void, blocking: true

    attach_function :stampeded_lease_job, [:pointer], :string
    attach_function :stampeded_lease_complete, [:pointer], :int32, blocking: true
    attach_function :stampeded_lease_fail, %i[pointer string int64], :int32, blocking: true
    attach_function :stampeded_lease_timed_out, %i[pointer string], :int32, blocking: true
    attach_function :stampeded_lease_requeue, [:pointer], :int32, blocking: true
    attach_function :stampeded_lease_discard, %i[pointer string], :int32, blocking: true
    attach_function :stampeded_lease_free, [:pointer], :void, blocking: true

    attach_function :stampeded_lifecycle_states, [], :string
    attach_function :stampeded_lifecycle_events, [], :string
    attach_function :stampeded_lifecycle_apply, %i[string string pointer], :int32
    attach_function :stampeded_cron_check, %i[string string pointer], :int32

    attach_function :stampeded_steps_load, [:string], :pointer
    attach_function :stampeded_steps_free, [:pointer], :void
    attach_function :stampeded_steps_completed, %i[pointer string], :int32
    attach_function :stampeded_steps_value, %i[pointer string], :string
    attach_function :stampeded_steps_cursor, %i[pointer string], :string
    attach_function :stampeded_steps_complete, %i[pointer string string], :int32
    attach_function :stampeded_steps_set_cursor, %i[pointer string string], :int32
    attach_function :stampeded_steps_json, [:pointer], :string

    module_function

    def version = stampeded_version
    def log_init(filter)
      status = stampeded_log_init(filter)
      raise Error, last_error if status == ERROR

      status == OK
    end

    def last_error = stampeded_last_error || "unknown error"

    def ms(seconds) = (seconds.to_f * 1000).round.clamp(0, 0xFFFF_FFFF)
    def text(value) = value.to_s.encode("UTF-8", invalid: :replace, undef: :replace).delete("\0")

    def lifecycle_states = stampeded_lifecycle_states.split(",").map(&:to_sym)

    def lifecycle_events = stampeded_lifecycle_events.split(",").map(&:to_sym)

    def lifecycle_apply(state, event)
      out = FFI::MemoryPointer.new(:pointer)
      case stampeded_lifecycle_apply(state.to_s, event.to_s, out)
      when OK then out.read_pointer.read_string.to_sym
      when NONE then nil
      else raise Error, last_error
      end
    end

    def cron_check(expr, timezone)
      out = FFI::MemoryPointer.new(:pointer)
      raise Error, last_error if stampeded_cron_check(text(expr), text(timezone), out) == ERROR

      out.read_pointer.read_string
    end

    class Steps
      def initialize(checkpoint)
        pointer = Native.stampeded_steps_load(checkpoint.nil? ? nil : JSON.generate(checkpoint))
        raise Error, Native.last_error if pointer.null?

        @pointer = FFI::AutoPointer.new(pointer, Native.method(:stampeded_steps_free))
      end

      def completed?(name) = check(Native.stampeded_steps_completed(@pointer, name.to_s)) == OK

      def value(name) = decode(Native.stampeded_steps_value(@pointer, name.to_s))

      def cursor(name) = decode(Native.stampeded_steps_cursor(@pointer, name.to_s))

      def complete(name, json) = check(Native.stampeded_steps_complete(@pointer, name.to_s, json))

      def set_cursor(name, json) = check(Native.stampeded_steps_set_cursor(@pointer, name.to_s, json))

      def to_h = JSON.parse(Native.stampeded_steps_json(@pointer))

      private

      def check(status)
        raise Error, Native.last_error if status == ERROR

        status
      end

      def decode(json) = json && JSON.parse(json)
    end
    class Puller
      attr_reader :worker_id

      def initialize(url:, queues:, except:, max_leases:, batch_size:, poll_interval:, heartbeat_interval:)
        @client = Native.stampeded_connect(url)
        raise Error, Native.last_error if @client.null?

        options = PullerOptions.new
        queue_spec  = FFI::MemoryPointer.from_string(Native.text(queues))
        except_spec = except && FFI::MemoryPointer.from_string(Native.text(except))
        options[:queues]                = queue_spec
        options[:except_queues]         = except_spec || FFI::Pointer::NULL
        options[:max_leases]            = max_leases
        options[:batch_size]            = batch_size
        options[:poll_interval_ms]      = Native.ms(poll_interval)
        options[:heartbeat_interval_ms] = Native.ms(heartbeat_interval)

        @puller = Native.stampeded_puller_start(@client, options)
        if @puller.null?
          message = Native.last_error
          close
          raise Error, "could not start the worker: #{message}"
        end
        @worker_id = Native.stampeded_puller_worker_id(@puller)
      end

      def next(timeout)
        out = FFI::MemoryPointer.new(:pointer)
        case Native.stampeded_puller_next(@puller, Native.ms(timeout), out)
        when OK then Lease.new(out.read_pointer)
        when NONE then nil
        else raise Error, Native.last_error
        end
      end

      def stop(drain)
        raise Error, Native.last_error if Native.stampeded_puller_stop(@puller, Native.ms(drain)) == ERROR
      end

      def close
        Native.stampeded_puller_free(@puller) if @puller && !@puller.null?
        Native.stampeded_client_free(@client) if @client && !@client.null?
        @puller = @client = nil
      end
    end

    class Lease
      attr_reader :row

      def initialize(pointer)
        @pointer = pointer
        @row = JSON.parse(Native.stampeded_lease_job(pointer))
      end

      def settled? = @pointer.nil?

      def complete! = settle { Native.stampeded_lease_complete(it) }
      def fail!(message, retry_in_ms: nil)
        settle { Native.stampeded_lease_fail(it, Native.text(message), retry_in_ms || -1) }
      end

      def requeue! = settle { Native.stampeded_lease_requeue(it) }

      def timed_out!(message) = settle { Native.stampeded_lease_timed_out(it, Native.text(message)) }

      def discard!(message) = settle { Native.stampeded_lease_discard(it, Native.text(message)) }

      def release!
        return if settled?

        pointer = @pointer
        @pointer = nil
        Native.stampeded_lease_free(pointer)
      end

      private

      def settle
        raise Error, "lease already settled" if settled?

        pointer = @pointer
        @pointer = nil
        raise Error, Native.last_error if yield(pointer) == ERROR
      end
    end
  end
end
