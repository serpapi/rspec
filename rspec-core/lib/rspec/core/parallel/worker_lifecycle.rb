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
          @load_themselves = true
          # A spawned worker starts Ruby and loads the `--require`d files first.
          @boot_allowance = Parallel.spawning? ? Process.times.then { |times| times.utime + times.stime } : 0.0
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
          clean_up
        end

      private
        # Whatever stopping the workers did, also when it failed.
        def clean_up
          Parallel.restore_signals(@signal_handlers)
          WorkerOutput.remove_directory(@output_dir)
          @reaper&.finish
        end

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
          # Shares more memory with forked workers; spawned ones share none.
          @process.warmup if @process.respond_to?(:warmup) && !Parallel.spawning?
          @output_dir ||= WorkerOutput.make_directory
          # Spawned workers watch the coordinator's pid themselves.
          @reaper ||= @reaper_class&.start(@output_dir) unless Parallel.spawning?

          @worker_count.times do |index|
            break unless start_worker(index + 1, units)
          end
        end

        # @return [WorkerHandle, nil] the worker, if it started
        def start_worker(number, units, starter=@spawn, generation=1)
          worker = WorkerHandle.new(number, @output_dir, generation)
          worker.loading = units.nil?
          @workers << worker
          worker.pid = starter.call(worker, @workers)
          @reaper&.watch(worker.pid)
          worker.close_child_ends
          worker.starting = true
          # A worker forked before loading gets the loading deadline instead.
          worker.start_deadline = now + @configuration.parallel_shutdown_timeout unless worker.loading
          worker
        rescue *OUT_OF_RESOURCES => e
          if worker
            @workers.delete(worker)
            worker.close_all
          end
          RSpec.warning "Started #{@workers.size} of #{@worker_count} parallel workers: the next could not be started " \
                        "(#{e.class}: #{e.message}). #{@workers.empty? ? 'Running serially' : 'Going on without it'}",
                        :call_site => nil
          nil
        end

        # A worker lost while it ran groups is replaced by one with its
        # number -- and so its `TEST_ENV_NUMBER` -- while groups wait to run
        # and the run goes on: not once it stops early or ends by `exit` or a
        # signal, and no more often than there are workers. The examples the
        # lost worker failed stay failed.
        def replace(worker)
          return unless worker.lost && accepting_work? && !@queue.empty? && @exit_status.nil? && @termination_signal.nil?
          return no_more_replacements if @replacements >= @worker_count
          return unless (starter = replacement_starter)

          @replacements += 1
          replacement = start_worker(worker.number, @load_themselves ? nil : @units, starter, @replacements + 1)
          return unless replacement

          replacement.deadline = now + @load_allowance if replacement.loading
          report "#{replacement.description} was started in place of the lost worker (pid #{worker.pid})" \
                 "#{' and loads the spec files first' if replacement.loading}."
        end

        # Workers that loaded the spec files themselves are replaced the same
        # way, by a new process when they were forked: forking this process,
        # which has loaded them, is what the run is configured not to do.
        def replacement_starter
          return @spawn if !@load_themselves || Parallel.spawning?

          method(:spawn_worker) if Runner.command_line
        end

        def no_more_replacements
          return if @replacements_exhausted

          @replacements_exhausted = true
          report "No further lost workers are replaced: the run has started as many in place of lost ones as it " \
                 "has workers (#{@replacements})."
        end

        def fork_worker(worker, workers)
          worker.fork_worker(@process, @units, workers, :signal_handlers => @signal_handlers)
        end

        # The `rspec` command this process runs, run again by the same Ruby
        # with this process's load path.
        def spawn_worker(worker, _workers)
          script, args = Runner.command_line
          command = [RbConfig.ruby, "-I#{$LOAD_PATH.join(File::PATH_SEPARATOR)}", script, *args]
          environment = SpawnedWorker.environment(worker.number, Process.pid, @configuration.seed, worker.output&.paths)
          worker.spawn_worker(@process, command, environment)
        end

        # What a worker that loaded the spec files itself printed while it
        # did is what this process printed when it loaded them.
        def output(worker, sizes)
          return unless worker.output
          return worker.output.skip(sizes) if worker.loading

          worker.output.read(sizes) { |stream, bytes| show_output(worker, stream, bytes) }
        end

        # What is left in a worker's output files once it has gone: what it
        # printed after it last reported, such as a crash report or what its
        # `at_exit` handlers printed.
        def drain_output(worker)
          return unless worker.output

          worker.output.read { |stream, bytes| show_output(worker, stream, bytes) }
          worker.output.close
        end

        # A worker dismissed before the run used it has only loaded the spec
        # files, which this process did too; before the run reports, no
        # worker has run anything.
        def show_output(worker, stream, bytes)
          return if worker.state == :dismissed || @sequencer.nil?

          @sequencer.output(worker.number, stream, bytes)
        end

        # Anything it sends shows it got past forking and runs RSpec's code.
        def worker_started(worker)
          worker.starting = false
          worker.clear_start_deadline
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
          deadline = now + (now - @started_at) + @boot_allowance + @configuration.parallel_shutdown_timeout
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
            drain_output(worker)
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
