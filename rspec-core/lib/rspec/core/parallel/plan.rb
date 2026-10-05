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
          count = worker_count
          return unless count && count >= 2

          # Assigned first, so that workers forked before a `fork` that fails
          # are dismissed too.
          @coordinator = Coordinator.new(@runner, count)
          @coordinator.start
        end

        # @return [Coordinator, nil] the coordinator to run the loaded groups,
        #   or `nil` for a serial run
        def coordinator_for(example_groups)
          return assign(example_groups) if @decided
          return Parallel.coordinator_for(@runner, example_groups) if @configuration.parallel_preload?

          # Too late to fork before loading, and forking after loading is
          # what this run was configured not to do.
          requested = Parallel.requested_worker_count(@configuration)
          warn_late if requested && requested >= 2
          nil
        end

        # Ends the workers forked before loading that the run did not use.
        def dismiss
          @coordinator&.dismiss
        end

      private

        # Each worker loads every spec file, so there are no more of them
        # than spec files -- a spec file usually defines one top-level group
        # -- but two for a single file, which may define several.
        def worker_count
          requested = Parallel.requested_worker_count(@configuration)
          return requested unless requested && requested > 2

          @configuration.files_to_run.uniq.size.clamp(2, requested)
        end

        def assign(example_groups)
          return nil unless @coordinator

          units = Parallel.units_for(example_groups, @configuration)
          if units.size < 2
            dismiss
            return nil
          end

          @coordinator.assign(example_groups, units)
          @coordinator
        end

        def warn_late
          what = @postponed_by || '`config.parallel_preload = false`'
          RSpec.warning "Ignoring `--parallel`: #{what} was set while the spec files were loading, too late to fork " \
                        "the workers before them as `config.parallel_preload = false` asks. Set it before they " \
                        "load: on the command line, in `.rspec` or in a `--require`d file. Running serially.",
                        :call_site => nil
        end
      end
    end
  end
end
