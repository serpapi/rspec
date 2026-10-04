RSpec::Support.require_rspec_core "parallel/channel"
RSpec::Support.require_rspec_core "parallel/remote_exception"
RSpec::Support.require_rspec_core "parallel/worker_reporter"
RSpec::Support.require_rspec_core "parallel/worker"
RSpec::Support.require_rspec_core "parallel/sequencer"
RSpec::Support.require_rspec_core "parallel/results"
RSpec::Support.require_rspec_core "parallel/suite_index"
RSpec::Support.require_rspec_core "parallel/worker_handle"
RSpec::Support.require_rspec_core "parallel/coordinator"

module RSpec
  module Core
    # Runs top-level example groups across worker processes forked from the
    # process that loaded the suite (`rspec --parallel[=COUNT]`,
    # `config.parallel_workers`). Formatters run in the coordinating process
    # only and receive the notifications of a normal run; see {Coordinator}.
    # @private
    module Parallel
      DURATION = /(?:(\d+) minutes? )?([\d.]+) seconds?/

      # @return [Coordinator, nil] a coordinator for the groups, or `nil` when
      #   the run should proceed serially (not requested, not possible or not
      #   worth it). Warns when a requested parallel run is not possible.
      def self.coordinator_for(runner, example_groups)
        requested = requested_worker_count(runner.configuration)
        return nil unless requested

        units = units_for(example_groups, runner.configuration)
        count = [requested, units.size].min
        return nil if count < 2

        Coordinator.new(runner, example_groups, units, count)
      end

      # @private
      def self.requested_worker_count(configuration)
        requested = configuration.parallel_workers
        return nil if requested.nil? || configuration.dry_run?

        unless RSpec::Support::RubyFeatures.fork_supported?
          RSpec.warning "Ignoring `--parallel`: it needs `fork`, which is not available on #{RUBY_ENGINE} " \
                        "(#{RUBY_PLATFORM}). Running serially.", :call_site => nil
          return nil
        end

        if ENV.key?('TEST_ENV_NUMBER')
          RSpec.warning "Ignoring `--parallel`: this process is already a worker of a parallel test run " \
                        "(TEST_ENV_NUMBER is set). Running serially.", :call_site => nil
          return nil
        end

        requested == :auto ? default_worker_count : requested
      end

      # @return [Integer] the number of processors
      def self.default_worker_count
        require 'etc'
        Etc.nprocessors
      end

      # The groups to dispatch: top-level groups with at least one example to
      # run, longest first when a previous run recorded their timings in the
      # example status persistence file, so that the slowest group is never
      # the one that starts last. Groups without timings go first because
      # nothing is known about them.
      # @private
      def self.units_for(example_groups, configuration)
        units = example_groups.select { |group| group.descendant_filtered_examples.any? }
        run_times = recorded_run_times(configuration)
        return units if run_times.empty?

        timed = {}
        units.each do |group|
          ids = group.descendant_filtered_examples.map(&:id)
          timed[group] = ids.sum { |id| run_times.fetch(id, 0.0) } if ids.any? { |id| run_times.key?(id) }
        end

        unknown, known = units.partition { |group| !timed.key?(group) }
        unknown + known.each_with_index.sort_by { |group, index| [-timed[group], index] }.map(&:first)
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
