# The runner itself -- parallel/coordinator and the files it requires -- is
# loaded once a run is going to fork workers, so that serial runs skip it.
RSpec::Support.require_rspec_core "parallel/plan"

module RSpec
  module Core
    # Runs top-level example groups across worker processes forked from the
    # process that loaded the suite (`rspec --parallel[=COUNT]`,
    # `config.parallel_workers`). Formatters run in the coordinating process
    # only and receive the notifications of a normal run; see {Coordinator}.
    # @private
    module Parallel
      DURATION = /(?:(\d+) minutes? )?([\d.]+) seconds?/

      # Raised -- as the failure of the example a worker was running, or as
      # an error outside of examples -- when a parallel worker is lost:
      # it crashed, was killed, or had to be terminated because it did not
      # stop or exit in time.
      class WorkerError < StandardError; end

      # What a worker is given to run: a top-level example group, or only the
      # examples of it with these ids -- the ones a lost worker had not
      # started -- which it runs as `rspec path[ids]` would: those examples,
      # and the context hooks of the groups that contain them.
      # @private
      Unit = Struct.new(:group, :example_ids, :retried) do
        # @return [Array<String>] the ids of the examples to run
        def ids
          example_ids || group.descendant_filtered_examples.map(&:id)
        end

        def description
          "#{group.description} (#{group.location})"
        end
      end

      # @return [Coordinator, nil] a coordinator for the loaded groups, its
      #   workers to be forked once it runs, or `nil` when the run should
      #   proceed serially (not requested, not possible or not worth it).
      #   Warns when a requested parallel run is not possible. See {Plan} for
      #   workers forked before loading.
      def self.coordinator_for(runner, example_groups)
        requested = requested_worker_count(runner.configuration)
        return nil unless requested

        units = units_for(example_groups, runner.configuration)
        count = [requested, units.size].min
        return nil if count < 2

        RSpec::Support.require_rspec_core "parallel/coordinator"
        Coordinator.new(runner, count).tap { |coordinator| coordinator.assign(example_groups, units) }
      end

      # Fingerprints of the examples a process loaded -- ids and locations of
      # those that will run, in load order -- per spec file, so that a worker
      # that loaded the spec files itself is used only if it loaded the same
      # examples as the coordinating process, and otherwise the files that
      # differ can be named. Descriptions are left out: results are reported
      # on the coordinating process's examples, so one computed at load time
      # (from the time, a temporary file's path, ...) is simply its own.
      # @return [Hash{String => String}] by the file of the top-level group
      def self.suite_digest(world)
        require 'digest/sha2'
        digests = Hash.new { |hash, file| hash[file] = Digest::SHA256.new }
        world.example_groups.each do |group|
          digest = digests[group.metadata[:file_path]]
          group.descendant_filtered_examples.each { |example| digest << example.id << "\0" << example.location << "\n" }
        end
        digests.transform_values(&:hexdigest)
      end

      # @private
      def self.requested_worker_count(configuration)
        requested = configuration.parallel_workers
        return nil if requested.nil? || configuration.dry_run?

        if spawning? && !Runner.command_line
          RSpec.warning "Ignoring `--parallel`: without `fork` (#{RUBY_ENGINE}, #{RUBY_PLATFORM}) its workers are new " \
                        "`rspec` processes, which takes a run started by the `rspec` command rather than by " \
                        "`RSpec::Core::Runner.run`. Running serially", :call_site => nil
          return nil
        end

        if ENV.key?('TEST_ENV_NUMBER')
          RSpec.warning "Ignoring `--parallel`: this process is already a worker of a parallel test run " \
                        "(TEST_ENV_NUMBER is set). Running serially", :call_site => nil
          return nil
        end

        requested == :auto ? default_worker_count : requested
      end

      # Without `fork`, workers are new `rspec` processes, which load the spec
      # files themselves.
      def self.spawning?
        !RSpec::Support::RubyFeatures.fork_supported?
      end

      # @return [Integer] the number of processors
      def self.default_worker_count
        require 'etc'
        Etc.nprocessors
      end

      # The units to dispatch: top-level groups with at least one example to
      # run -- or, for a group with `parallel_split` metadata, that many
      # parts of it -- longest first when a previous run recorded their
      # timings in the example status persistence file, so that the slowest
      # unit is never the one that starts last. Units without timings go
      # first because nothing is known about them.
      # @return [Array<Unit>]
      # @private
      def self.units_for(example_groups, configuration)
        run_times = recorded_run_times(configuration)
        units = example_groups.flat_map do |group|
          group.descendant_filtered_examples.empty? ? [] : split(group, run_times)
        end
        return units if run_times.empty?

        timed = {}
        units.each do |unit|
          ids = unit.ids
          timed[unit] = ids.sum { |id| run_times.fetch(id, 0.0) } if ids.any? { |id| run_times.key?(id) }
        end

        unknown, known = units.partition { |unit| !timed.key?(unit) }
        unknown + known.each_with_index.sort_by { |unit, index| [-timed[unit], index] }.map(&:first)
      end

      # The orderings that order part of a group's examples the way they
      # order all of them.
      SPLITTABLE_ORDERINGS = [Ordering::Identity, Ordering::Random, Ordering::RecentlyModified].freeze

      # A group tagged `parallel_split: N` is run as N parts -- no more than it
      # has examples -- each a slice of the order its examples run in,
      # balanced by their recorded run times (by their number, without).
      # @return [Array<Unit>]
      # @private
      def self.split(group, run_times)
        parts = group.metadata[:parallel_split]
        return [Unit.new(group)] unless parts

        unless parts.is_a?(Integer) && parts >= 2
          RSpec.warning "Ignoring `parallel_split: #{parts.inspect}` on #{group.description} (#{group.location}): it is " \
                        "the number of parts to run the group's examples in, at least 2", :call_site => nil
          return [Unit.new(group)]
        end

        unless group.descendants.all? { |each| SPLITTABLE_ORDERINGS.any? { |kind| each.ordering_strategy.is_a?(kind) } }
          RSpec.warning "Not splitting #{group.description} (#{group.location}) into parts: it orders examples with a " \
                        "custom ordering, which may not order part of them the way it orders all of them",
                        :call_site => nil
          return [Unit.new(group)]
        end

        ids = execution_order(group)
        return [Unit.new(group)] if ids.size < 2

        slices(ids, [parts, ids.size].min, run_times).map { |slice| Unit.new(group, slice) }
      end

      # @return [Array<String>] the ids of the group's examples in the order a
      #   run of the group runs them in
      # @private
      def self.execution_order(group)
        strategy = group.ordering_strategy
        strategy.order(group.filtered_examples).map(&:id) +
          strategy.order(group.children).flat_map { |child| execution_order(child) }
      end

      # Cuts `ids` into `count` consecutive, non-empty slices of about the
      # same total run time -- each example goes to the slice its midpoint
      # falls in -- where an example without a recorded time counts as the
      # mean of those with one, and with no time recorded at all every
      # example counts the same.
      # @private
      def self.slices(ids, count, run_times)
        recorded = ids.map { |id| run_times[id] }.compact
        mean = recorded.empty? ? 0.0 : recorded.sum / recorded.size
        weights = ids.map { |id| run_times.fetch(id, mean) }
        weights = Array.new(ids.size, 1.0) unless weights.sum > 0
        total = weights.sum

        slices = [[]]
        done = 0.0
        ids.each_with_index do |id, index|
          more = count - slices.size
          wanted = ((done + (weights[index] / 2.0)) * count / total).floor
          slices << [] if !slices.last.empty? && more > 0 && (wanted >= slices.size || ids.size - index <= more)
          slices.last << id
          done += weights[index]
        end
        slices
      end

      # @private
      def self.recorded_run_times(configuration)
        path = configuration.example_status_persistence_file_path
        return {} unless path

        ExampleStatusPersister.load_from(path).each_with_object({}) do |example, hash|
          seconds = parse_duration(example[:run_time])
          hash[example[:example_id]] = seconds if seconds
        end
      rescue SystemCallError
        {}
      end

      # @private
      def self.parse_duration(string)
        match = DURATION.match(string.to_s)
        return nil unless match

        (match[1].to_i * 60) + match[2].to_f
      end

      # @private
      def self.seconds(duration)
        Formatters::Helpers.pluralize(Formatters::Helpers.format_seconds(duration), 'second')
      end

      # @private
      def self.monotonic_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      # Longer than any wait of the coordinator's loop or of a worker's
      # watchdog: a longer gap means the process was not running -- stopped
      # with the whole run (Ctrl-Z), the machine asleep -- and that time does
      # not count towards a deadline.
      PAUSE_TOLERANCE = 1.0

      # Sleeps until this process has been running for `seconds`.
      # @private
      def self.sleep_running(seconds, slice=0.25)
        remaining = seconds
        while remaining > 0
          step = [remaining, slice].min
          started = monotonic_time
          sleep step
          elapsed = monotonic_time - started
          remaining -= elapsed > step + PAUSE_TOLERANCE ? step : elapsed
        end
      end

      # A `Time` as whole seconds and nanoseconds, the way workers send it:
      # marshaling a local `Time` asks libc for its UTC offset, and in a
      # process forked on macOS each such call reloads the time zone file
      # (tens of microseconds, twice per example).
      # @private
      def self.time_to_wire(time)
        time && [time.tv_sec, time.tv_nsec]
      end

      # @private
      def self.time_from_wire(value)
        value.is_a?(Array) ? ::Time.at(value[0], value[1], :nsec) : value
      end

      # Whatever the formatters wrote before `start` (seed, run options) may
      # still be buffered in streams that are not `$stdout`; forked workers
      # would write it again when they exit.
      # @private
      def self.flush_output(configuration)
        streams = configuration.formatters.map { |formatter| formatter.output if formatter.respond_to?(:output) }
        streams.push(configuration.output_stream, configuration.error_stream, configuration.deprecation_stream)
        streams.push($stdout, $stderr)
        streams.each { |stream| stream.flush if stream.respond_to?(:flush) }
      end

      # Handlers that end the process: Ruby's and the system's defaults. Any
      # other (`IGNORE`, `nil` -- also an ignored signal --, `EXIT`, a proc)
      # stays in force.
      ENDS_PROCESS = %w[DEFAULT SYSTEM_DEFAULT].freeze

      # Installs `handler` for each signal the platform has that would end the
      # process. Ignored signals (as under `nohup`) and handlers the suite
      # installed are left in place.
      # @return [Hash{String => Object}] the handlers it replaced
      # @private
      def self.trap_signals(signals, &handler)
        signals.each_with_object({}) do |signal, replaced|
          next unless Signal.list.key?(signal)

          previous = trap(signal) { handler.call(signal) }
          if ENDS_PROCESS.include?(previous)
            replaced[signal] = previous
          else
            trap(signal, previous)
          end
        end
      end

      # @private
      def self.restore_signals(handlers)
        handlers.each { |signal, previous| trap(signal, previous) }
      end

      # @return the value when `Marshal` can serialize it, otherwise what the
      #   block makes of it
      # @private
      def self.marshal_safe(value)
        Marshal.dump(value)
        value
      rescue StandardError
        yield value
      end
    end
  end
end
