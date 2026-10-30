# frozen_string_literal: true

module Stampeded
  module Lifecycle
    module_function

    def states = @states ||= native.lifecycle_states

    def events = @events ||= native.lifecycle_events

    def apply(from, event) = native.lifecycle_apply(from, event)

    def legal?(from, event)
      !apply(from, event).nil?
    rescue Error
      false
    end

    def events_from(state) = events.select { legal?(state, it) }

    # No worker moves it again; only an operator's resurrect does.
    def terminal?(state) = events_from(state).all?(:resurrect)

    def claimable?(state) = legal?(state, :claim)

    def native
      require "stampeded/native"
      Native
    end
  end
end
