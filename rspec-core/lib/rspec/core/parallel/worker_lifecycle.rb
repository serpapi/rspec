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
        # What the system answers when it will not give this process another
        # worker right now: no file descriptors for its pipes, no process for
        # it. The run goes on with the workers it has.
        OUT_OF_RESOURCES = [Errno::EMFILE, Errno::ENFILE, Errno::EAGAIN, Errno::ENOMEM].freeze

        # Between checks for workers that exited without closing their
        # results pipe (a child of theirs holds it open, say); one whose pipe
        # closed is checked for at once.
        REAP_INTERVAL = 0.05

        # Forks the workers before the spec files are loaded; each loads them
        # itself and reports what it loaded.
        def start
          @started_at = now
          start_workers(nil)
        end

        # @return [Boolean] whether any worker could be started
        def started?
          !@workers.empty?
        end

        # Ends the workers that are not done -- workers forked before loading
        # when the run ends early or goes on serially: they have run nothing.
        def dismiss
          live = live_workers
          live.each { |worker| dismiss_worker(worker) }
          give_up_unless_gone(live, @kill_grace)
        end

      private

        # Rather than a `waitpid` for every worker on every wake-up of the
        # loop, which comes every few frames while results stream in.
        def reap_workers_when_due
          return unless @reap_due || now >= @next_reap_at

          @reap_due = false
          @next_reap_at = now + REAP_INTERVAL
          reap_workers
        end

        # Moves each deadline back by the time the coordinator's loop was not
        # running, beyond its own waits, since the deadline was set: the run
        # was stopped (Ctrl-Z) or the machine slept, its workers with it.
        def account_for_pauses
          current = now
          turn_started = @last_turn_at
          @last_turn_at = current

          @workers.each do |worker|
            next unless worker.deadline

            since = [turn_started, worker.deadline_set_at].compact.max
            paused = current - since - @tick - Parallel::PAUSE_TOLERANCE
            worker.postpone(paused) if paused > 0
          end
        end

        # Without `units` the workers load the spec files themselves. Each
        # worker's pipes are opened just before it is forked, so that the
        # parent never holds more than its own ends of the earlier ones.
        def start_workers(units)
          Parallel.flush_output(@configuration)
          @process.warmup if @process.respond_to?(:warmup)

          @worker_count.times do |index|
            break unless start_worker(index + 1, units)
          end
        end

        # @return [Boolean] whether the worker started
        def start_worker(number, units)
          worker = WorkerHandle.new(number)
          worker.loading = units.nil?
          @workers << worker
          worker.pid = @spawn.call(worker, @workers)
          worker.close_child_ends
          true
        rescue *OUT_OF_RESOURCES => e
          if worker
            @workers.delete(worker)
            worker.close_all
          end
          RSpec.warning "Started #{@workers.size} of #{@worker_count} parallel workers: the next could not be started " \
                        "(#{e.class}: #{e.message}). #{@workers.empty? ? 'Running serially' : 'Going on without it'}",
                        :call_site => nil
          false
        end

        def fork_worker(worker, workers)
          worker.fork_worker(@process, @units, workers, :signal_handlers => @signal_handlers)
        end

        # Including those a failed `fork` left without a process.
        def live_workers
          @workers.each { |worker| worker.state = :exited if worker.pid.nil? }
          @workers.reject(&:done?)
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
          live = live_workers
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
