module RSpec
  module Core
    module Parallel
      # The reporter installed in a worker process. It has no formatters;
      # it forwards every notification -- RSpec's own and custom ones
      # published through `Reporter#publish` -- to the coordinator as
      # `Marshal`-safe frames referring to examples and groups by id. The
      # coordinator replays them onto its own reporter, so formatters run
      # exclusively in the coordinating process and receive real `Example`
      # and `ExampleGroup` objects.
      #
      # Listeners registered on this reporter from inside the worker (for
      # instance in a `before(:suite)` hook) are notified as usual before the
      # notification is forwarded.
      # @private
      class WorkerReporter < Reporter
        FORWARDED_AS_PAYLOAD = [:message, :deprecation].freeze
        EXAMPLE_OUTCOMES = [:example_passed, :example_failed, :example_pending].freeze
        RUN_LIFECYCLE = [:seed, :start, :stop, :start_dump, :dump_pending, :dump_failures, :deprecation_summary,
                         :dump_profile, :dump_summary, :close].freeze
        # Example metadata that is not compared before and after the example:
        # RSpec's own, which the coordinator keeps, and what the result
        # payload carries anyway.
        NOT_COMPARED = (Metadata::RESERVED_KEYS + [:pending, :skip, :extra_failure_lines]).to_h { |key| [key, true] }.freeze

        def initialize(configuration, results, jobs, world=RSpec.world)
          super(configuration)
          @results = results
          @jobs = jobs
          @world = world
          @group_started_at = nil
          @unforwarded_expectations = 0
          @metadata_before = nil
        end

        # @private
        # Called for every expectation by an rspec-expectations that counts
        # them; the count reaches the coordinator through
        # {#forward_expectation_count}.
        def increment_expectation_count
          @unforwarded_expectations += 1
        end

        # @private
        # Sends what was counted since the last call, if anything: the
        # coordinator adds it to its own reporter's count, if that keeps one.
        def forward_expectation_count
          return if @unforwarded_expectations.zero?

          @results.send([:expectations, @unforwarded_expectations])
          @unforwarded_expectations = 0
        end

        # @private
        # Suite hook failures and the like: the coordinator rebuilds the
        # exception and reports it through its own reporter, which keeps the
        # run's exit code and "errors occurred outside of examples" count right.
        def notify_non_example_exception(exception, context_description)
          @world.non_example_failure = true
          @results.send([:non_example_failure, RemoteException.payload_for(exception), context_description.to_s])
        end

        # @private
        # An example's result is forwarded with its outcome notification
        # (`example_passed`, `example_failed` or `example_pending`), which
        # follows `example_finished`, so that metadata worker listeners add
        # on either (such as `:extra_failure_lines`) reaches the coordinator.
        def notify(event, notification)
          super

          case event
          when :example_group_started  then forward_group_started(notification.group)
          when :example_group_finished then forward_group_finished(notification.group)
          when :example_started        then forward_example_started(notification.example)
          when *EXAMPLE_OUTCOMES       then forward_example_finished(notification.example)
          when :example_finished, *RUN_LIFECYCLE then nil
          when *FORWARDED_AS_PAYLOAD   then forward(event, notification)
          else forward_custom(event, notification)
          end
        end

      private

        def forward_group_started(group)
          @group_started_at = Parallel.monotonic_time if group.top_level?
          @results.send([:group_started, group.id, descriptions(group.metadata)])
        end

        def forward_group_finished(group)
          duration = nil
          if group.top_level? && @group_started_at
            duration = Parallel.monotonic_time - @group_started_at
            @group_started_at = nil
          end
          @results.send([:group_finished, group.id, duration])
        end

        def forward_example_started(example)
          @metadata_before = metadata_snapshot(example.metadata)
          @results.send([:example_started, example.id])
        end

        def forward_example_finished(example)
          @results.send([:example_finished, example.id, result_payload(example)])
          poll_for_quit
        end

        # The coordinator asks workers to stop between examples (fail-fast,
        # interrupt) by writing `[:quit]` on the jobs channel. Anything else
        # is left for the worker's job loop.
        def poll_for_quit
          @jobs.drain.each do |message, _size|
            if message.first == :quit
              @world.wants_to_quit = true
            else
              @jobs.unread(message)
            end
          end
        end

        def forward(event, notification)
          if event == :message
            @results.send([:message, Parallel.marshal_safe(notification.message, &:to_s)])
          else
            hash = notification.to_h
            @results.send([:deprecation, Parallel.marshal_safe(hash) { hash.transform_values { |v| v && v.to_s } }])
          end
        end

        def forward_custom(event, notification)
          options = notification.respond_to?(:to_h) ? notification.to_h : {}
          Marshal.dump(options)
          @results.send([:custom, event, options])
        rescue TypeError, ArgumentError, NoMethodError
          @results.send([:message, "Parallel worker #{@configuration.parallel_worker_number} could not forward " \
                                   "the custom `#{event}` notification because its options cannot be marshaled: " \
                                   "#{options.inspect[0, 500]}"])
        end

        def result_payload(example)
          result = example.execution_result
          metadata = example.metadata

          {
            :status              => result.status,
            :started_at          => Parallel.time_to_wire(result.started_at),
            :finished_at         => Parallel.time_to_wire(result.finished_at),
            :run_time            => result.run_time,
            :pending_message     => Parallel.marshal_safe(result.pending_message, &:to_s),
            :pending_fixed       => result.pending_fixed,
            :exception           => RemoteException.payload_for(result.exception),
            :pending_exception   => RemoteException.payload_for(result.pending_exception),
            :pending             => Parallel.marshal_safe(metadata[:pending], &:to_s),
            :skip                => Parallel.marshal_safe(metadata[:skip], &:to_s),
            :extra_failure_lines => Parallel.marshal_safe(metadata[:extra_failure_lines]) { |lines| Array(lines).map(&:to_s) },
            :metadata            => metadata_changes(metadata)
          }.merge(descriptions(metadata))
        end

        # Each value with its hash, so that a value changed in place counts as
        # changed too.
        def metadata_snapshot(metadata)
          metadata.each_with_object({}) do |(key, value), snapshot|
            snapshot[key] = [value, hash_of(value)] unless NOT_COMPARED.key?(key)
          end
        end

        # @return [Hash, nil] what the example, its hooks or a worker listener
        #   did to its metadata: keys set (`:set`) or deleted (`:deleted`),
        #   each as the key's `inspect` and its own `Marshal` dump -- so that
        #   one the coordinator cannot load (its class is not loaded there)
        #   spoils only itself -- and keys set to values that cannot be
        #   marshaled (`:unsent`, with the reason), which the coordinator
        #   leaves as they were.
        def metadata_changes(metadata)
          before = @metadata_before || {}
          @metadata_before = nil
          changes = {}
          metadata.each do |key, value|
            next if NOT_COMPARED.key?(key) || unchanged?(before[key], value)

            if (dump = marshaled([key, value]))
              (changes[:set] ||= []) << [key.inspect, dump]
            else
              (changes[:unsent] ||= {})[key.inspect] = "#{value.class.name} cannot be marshaled"
            end
          end
          deleted = deleted_keys(before, metadata)
          changes[:deleted] = deleted unless deleted.empty?
          changes.empty? ? nil : changes
        end

        def deleted_keys(before, metadata)
          before.keys.reject { |key| metadata.key?(key) }.filter_map { |key| (dump = marshaled(key)) && [key.inspect, dump] }
        end

        def unchanged?(snapshot, value)
          snapshot && snapshot[0].equal?(value) && snapshot[1] == hash_of(value)
        end

        def marshaled(value)
          Marshal.dump(value)
        rescue StandardError
          nil
        end

        def hash_of(value)
          value.hash
        rescue StandardError
          nil
        end

        # As this worker has them: generated by the matcher of a one-liner
        # (`it { is_expected.to ... }`) once the example ran, or computed
        # while the worker loaded the spec files itself.
        def descriptions(metadata)
          { :description => String.new(metadata[:description].to_s),
            :full_description => String.new(metadata[:full_description].to_s) }
        end
      end
    end
  end
end
