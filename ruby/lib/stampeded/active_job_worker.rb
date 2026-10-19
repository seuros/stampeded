# frozen_string_literal: true

require "stampeded"

module Stampeded
  class ActiveJobWorker < Worker
    def initialize(**kwargs)
      super
      @handlers.default_proc = proc { |_h, _task| method(:execute_active_job) }
    end

    private

    def execute_active_job(job)
      payload = Envelope.to_active_job_payload(id: job.id, queue: job.queue, task: job.task,
                                               args: job.args, meta: job.meta, checkpoint: job.checkpoint)

      klass = payload["job_class"]
      begin
        Object.const_get(klass)
      rescue NameError
        raise UnknownTask, "#{klass} is not defined in this worker"
      end

      Current.with(job) { ActiveJob::Base.execute(payload) }
    end
  end
end
