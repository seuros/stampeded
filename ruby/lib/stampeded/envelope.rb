# frozen_string_literal: true

module Stampeded
  module Envelope
    module_function

    def tasks
      @tasks ||= {}
    end

    def register(task, job_class)
      name = task.to_s
      raise ArgumentError, "a task name cannot be blank" if name.strip.empty?

      owner = tasks[name]
      if owner && owner != job_class.name
        raise ArgumentError, "task #{name.inspect} is already #{owner}'s; #{job_class.name} cannot claim it too"
      end

      tasks[name] = job_class.name
    end

    def from_active_job(job)
      build(job.class, job.arguments, job.serialize.except("arguments", "provider_job_id"))
    end

    # [task, args, meta] for `arguments` as `perform` takes them: plain keywords
    # become portable `args`, anything else rides in ActiveJob's own format.
    def build(job_class, arguments, context)
      task = job_class.try(:stampeded_task)
      kwargs = standard_arguments(arguments)

      if kwargs
        [task || job_class.name, kwargs, { "activejob" => context }]
      elsif task
        raise PortableArgumentsError.new(job_class.name, task)
      else
        context["arguments"] = ActiveJob::Arguments.serialize(arguments)
        [job_class.name, {}, { "activejob" => context }]
      end
    end

    def to_active_job_payload(id:, queue:, task:, args:, meta:, checkpoint: nil)
      context = meta.fetch("activejob", {})
      job_class = context["job_class"] || tasks[task] || task

      arguments =
        context.fetch("arguments") do
          kwargs = args.transform_keys(&:to_sym)
          kwargs.empty? ? [] : ActiveJob::Arguments.serialize([Hash.ruby2_keywords_hash(kwargs)])
        end

      context.merge(
        "job_class" => job_class,
        "job_id" => context["job_id"] || id,
        "queue_name" => queue,
        "arguments" => arguments,
        "executions" => context["executions"] || 0,
        "exception_executions" => context["exception_executions"] || {},
        "enqueued_at" => context["enqueued_at"] || Time.now.utc.iso8601
      ).merge(continuation(checkpoint))
    end

    def continuation(checkpoint)
      return {} unless checkpoint.is_a?(Hash) && checkpoint["activejob"].is_a?(Hash)

      checkpoint["activejob"].slice("continuation", "resumptions")
    end

    def standard_arguments(arguments)
      return {} if arguments.empty?
      return nil unless arguments.size == 1

      kwargs = arguments.first
      return nil unless kwargs.is_a?(Hash) && Hash.ruby2_keywords_hash?(kwargs)
      return nil unless plain?(kwargs)

      JSON.parse(JSON.generate(kwargs))
    end

    def plain?(value)
      case value
      when String, Integer, true, false, nil then true
      when Float then value.finite?
      when Array then value.all? { plain?(it) }
      when Hash then value.all? { |k, v| (k.is_a?(String) || k.is_a?(Symbol)) && plain?(v) }
      else false
      end
    end
  end
end
