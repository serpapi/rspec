module RSpec
  module Core
    module Parallel
      # Decides whether a run is parallel and when its workers are forked:
      # once the spec files have been loaded, as by default, or before, when
      # each worker is to load them itself (`config.parallel_preload = false`).
      # It keeps what it decided before loading for when they have loaded,
      # and the workers it forked until the run uses or dismisses them.
      # @private
      class Plan
        def initialize(runner)
          @runner = runner
          @configuration = runner.configuration
          @coordinator = nil
          @decided = false
          @postponed_by = nil
        end

        # Called once the run is configured, before the spec files are loaded.
        def before_loading
          return if @configuration.parallel_preload?
          return @postponed_by = '`config.parallel_workers`' if @configuration.parallel_workers.nil?
          # Such a run aborts, with `exit!`, as soon as the spec files have loaded.
          if @configuration.only_failures_but_not_configured?
            return @postponed_by = '`config.example_status_persistence_file_path` (for `--only-failures`)'
          end

          @decided = true
          requested = Parallel.requested_worker_count(@configuration)
          @coordinator = Coordinator.new(@runner, requested).tap(&:start) if requested && requested >= 2
        end

        # @return [Coordinator, nil] the coordinator to run the loaded groups,
        #   or `nil` for a serial run
        def coordinator_for(example_groups)
          unless @decided
            coordinator = Parallel.coordinator_for(@runner, example_groups)
            warn_late if coordinator && !@configuration.parallel_preload?
            return coordinator
          end
          return nil unless @coordinator

          units = Parallel.units_for(example_groups, @configuration)
          if units.size < 2
            dismiss
            return nil
          end

          @coordinator.assign(example_groups, units)
          @coordinator
        end

        # Ends the workers forked before loading that the run did not use.
        def dismiss
          @coordinator&.dismiss
        end

      private

        def warn_late
          RSpec.warning "The parallel workers are forked after the spec files were loaded, not before them as " \
                        "`config.parallel_preload = false` asks: #{@postponed_by || 'it'} was set while they were " \
                        "loading. Set it before they load: on the command line, in `.rspec` or in a `--require`d " \
                        "file.", :call_site => nil
        end
      end
    end
  end
end
