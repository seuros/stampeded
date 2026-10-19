# frozen_string_literal: true

module Stampeded
  module Current
    KEY = :__stampeded_job_context

    module_function

    def context = Thread.current[KEY]

    def with(context)
      previous = Thread.current[KEY]
      Thread.current[KEY] = context
      yield
    ensure
      Thread.current[KEY] = previous
    end
  end
end
