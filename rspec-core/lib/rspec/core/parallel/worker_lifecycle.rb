module RSpec
  module Core
    module Parallel
      # The part of the {Coordinator} that forks and ends the worker
      # processes. Workers are forked after the spec files have been loaded,
      # or before (`config.parallel_preload = false`): then each loads the
      # spec files itself and is given work only once it has reported that
      # its loading went the way the coordinator's did. Workers that never
      # got to run anything are dismissed, and every exit path terminates the
      # rest within the TERM/KILL grace periods.
      # @private
      module WorkerLifecycle
        # Forks the workers before the spec files are loaded; each loads them
        # itself and reports what it loaded.
        def start
          @started_at = now
          start_workers(nil)
        end

        # Ends the workers that are not done -- workers forked before loading
        # when the run ends early or goes on serially: they have run nothing.
        def dismiss
          live = @workers.reject(&:done?)
          live.each { |worker| dismiss_worker(worker) }
          give_up_unless_gone(live, @kill_grace)
        end

      private

        # Without `units` the workers load the spec files themselves.
        def start_workers(units)
          Parallel.flush_output(@configuration)
          @process.warmup if @process.respond_to?(:warmup)
          @worker_count.times { |index| @workers << WorkerHandle.new(index + 1) }

          @workers.each do |worker|
            worker.loading = units.nil?
            worker.pid = @spawn.call(worker, @workers)
            worker.close_child_ends
          end
        end

        def fork_worker(worker, workers)
          worker.fork_worker(@process, @units, workers, @signal_handlers)
        end

        # Workers started loading the spec files when this process did, so
        # they get as long again as it took, and the shutdown timeout on top,
        # to report what they loaded; they get work once they have.
        def expect_loading_workers
          @suite_digest = Parallel.suite_digest(@world)
          @load_failed = @world.non_example_failure
          deadline = now + (now - @started_at) + @configuration.parallel_shutdown_timeout
          @load_allowance = deadline - @started_at
          @workers.each { |worker| worker.deadline = deadline if worker.loading }
        end

        # A worker that loaded the spec files itself gets work only if its
        # loading went the way this process's did: no error this process did
        # not get, and the same examples.
        def loaded(worker, digest, failure)
          worker.loading = false
          worker.deadline = nil
          if failure && !@load_failed
            not_used(worker, "could not load the spec files: #{failure}")
          elsif digest != @suite_digest
            files = (digest.keys | @suite_digest.keys).reject { |file| digest[file] == @suite_digest[file] }
            not_used(worker, "loaded other examples than this process from #{files.first}" \
                             "#{" and #{Formatters::Helpers.pluralize(files.size - 1, 'more spec file')}" if files.size > 1} " \
                             "(examples defined depending on the process, the time or randomness?); with " \
                             "`--parallel-preload` workers are forked after the spec files are loaded")
          end
        end

        def not_used(worker, reason)
          error("#{worker.description} #{reason}, so it was not used.")
          dismiss_worker(worker)
        end

        # Ends a worker that has run nothing -- no example group, suite hook or
        # `at_exit` handler of this run -- so it has nothing to finish.
        def dismiss_worker(worker)
          worker.loading = false
          worker.state = :dismissed
          worker.results.close
          worker.signal(@process, 'KILL', :kill, now + @kill_grace)
        end

        # Runs on every exit path, so an exception in the coordinator can never
        # leave workers behind. Bounded by the same TERM/KILL grace periods.
        def terminate_remaining_workers
          @workers.each { |worker| worker.state = :exited if worker.pid.nil? }
          live = @workers.reject(&:done?)
          return if live.empty?

          live.each do |worker|
            worker.state = :terminating
            worker.signal(@process, 'TERM', :term, now + @term_grace)
          end
          wait_for_exit(live, @term_grace)
          live.reject(&:done?).each { |worker| worker.signal(@process, 'KILL', :kill, now + @kill_grace) }
          give_up_unless_gone(live, @kill_grace)
        end

        def give_up_unless_gone(workers, grace)
          wait_for_exit(workers, grace)
          workers.reject(&:done?).each do |worker|
            report "#{worker.description} survived SIGKILL; giving up on it."
            abort_unit(worker)
            worker.state = :abandoned
          end
        end

        def wait_for_exit(workers, grace)
          deadline = now + grace
          until workers.all?(&:done?) || now >= deadline
            sleep [@tick, 0.05].min
            reap_workers
          end
        end
      end
    end
  end
end
