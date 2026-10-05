module RSpec
  module Core
    module Parallel
      # The coordinating side of a parallel run. It forks the workers -- after
      # the suite has been loaded, so they inherit every example group, filter
      # and configuration, or before, so they load the spec files themselves
      # (`config.parallel_preload = false`) -- hands out top-level example
      # groups from a shared queue, streams the results onto the
      # configuration's reporter -- the one and only place formatters run --
      # and makes sure that no worker, however it misbehaves, can keep the run
      # from finishing.
      # @private
      class Coordinator
        include WorkerLifecycle

        TICK = 0.25
        # Between checks for workers that exited without closing their
        # results pipe (a child of theirs holds it open, say); one whose pipe
        # closed is checked for at once.
        REAP_INTERVAL = 0.05
        TERM_GRACE = 5.0
        KILL_GRACE = 5.0
        # Added to the shutdown timeout on the coordinator's side so that the
        # worker's own watchdog fires first and the thread dump comes from
        # the process that owns the threads.
        WATCHDOG_GRACE = 1.0
        SENT_TERM = ', so it was sent SIGTERM'
        # Handled by stopping the workers -- in time, even ones that cannot
        # tear down -- before the coordinator goes the way a serial run would.
        TERMINATION_SIGNALS = %w[TERM HUP].freeze

        attr_reader :worker_count

        def initialize(runner, worker_count, options={})
          @runner = runner
          @configuration = runner.configuration
          @world = runner.world
          @worker_count = worker_count
          @process = options.fetch(:process) { Process }
          @spawn = options.fetch(:spawn) { method(:fork_worker) }
          @tick = options.fetch(:tick, TICK)
          @term_grace = options.fetch(:term_grace, TERM_GRACE)
          @kill_grace = options.fetch(:kill_grace, KILL_GRACE)
          @watchdog_grace = options.fetch(:watchdog_grace, WATCHDOG_GRACE)
          @workers = []
          @results = Results.new
          @quitting = false
          @termination_signal = nil
          @signal_handlers = {}
          @started_at = nil
          @reap_due = true
          @next_reap_at = 0.0
        end

        # The groups to run. Workers forked before loading that are more than
        # the groups to run are dismissed.
        def assign(example_groups, units)
          @example_groups = example_groups
          @units = units
          @queue = (0...@units.size).to_a
          @unit_states = Array.new(@units.size, :pending)
          @workers.drop(@units.size).each { |worker| dismiss_worker(worker) }
        end

        # @return [Integer] the exit code for the run
        def run
          reporter = @configuration.reporter
          @signal_handlers = Parallel.trap_signals(TERMINATION_SIGNALS) { |signal| @termination_signal ||= signal }

          passed = reporter.report(@world.example_count(@example_groups)) do
            @sequencer = Sequencer.new(reporter, :apply_result => method(:apply_result))

            begin
              @workers.empty? ? start_workers(@units) : expect_loading_workers
              event_loop
            ensure
              terminate_remaining_workers
              @sequencer.finish
              @results.apply_all
              report_unrun_units
            end

            @unit_states.all? { |state| state == :passed }
          end

          # Terminated like a serial run would have been, once the workers are gone.
          raise SignalException, @termination_signal if @termination_signal

          @runner.exit_code(passed)
        ensure
          Parallel.restore_signals(@signal_handlers)
        end

      private

        def event_loop
          dispatch_work

          until @workers.all?(&:done?)
            return stop_for_signal if @termination_signal

            wait_for_results
            reap_workers_when_due
            request_quit if @world.wants_to_quit && !@quitting
            dispatch_work
            enforce_deadlines
            @sequencer.tick
          end
        end

        # A run stopped from outside -- a CI job's timeout, say -- most often
        # was stuck in an example, so it says what each worker was running.
        def stop_for_signal
          @workers.each { |worker| receive_from(worker) unless worker.done? }
          report "Received SIG#{@termination_signal}; stopping the workers."
          @workers.each { |worker| (running = worker.running_example_description) && report(running) }
        end

        def wait_for_results
          readers = @workers.reject { |worker| worker.done? || worker.results.eof? || worker.results.closed? }

          if readers.empty?
            # Each worker left is exiting (its results pipe is at its end) or
            # was given up on: all there is to do is reap it or enforce its
            # deadline, and an exiting worker is reapable within moments.
            @reap_due = true
            sleep [@tick, 0.01].min
            return
          end

          ready, = IO.select(readers.map { |worker| worker.results.io }, nil, nil, @tick)
          Array(ready).each do |io|
            receive_from(readers.find { |worker| worker.results.io.equal?(io) })
          end
        end

        def receive_from(worker)
          return if worker.results.closed?

          messages = drain(worker)
          messages.each { |message, bytes| handle_message(worker, message, bytes) }

          return unless worker.results.eof?

          # A worker whose results pipe closed while it was still running is
          # either about to be reaped or wedged; bound the wait for it.
          @reap_due = true
          worker.deadline ||= now + @configuration.parallel_shutdown_timeout + @watchdog_grace
        end

        def drain(worker)
          worker.results.drain
        rescue Channel::ProtocolError, TypeError, ArgumentError, IOError => e
          worker.results.close
          terminate(worker, "sent data the coordinator could not decode (#{e.class}: #{e.message})")
          []
        end

        def handle_message(worker, message, bytes)
          kind, *rest = message
          number = worker.number
          case kind
          when :group_started    then index.with_group(rest[0]) { |g| group_started(number, g, rest[1], bytes) }
          when :group_finished   then index.with_group(rest[0]) { |g| @sequencer.group_finished(number, g, rest[1], bytes) }
          when :example_started  then index.with_example(rest[0]) { |e| example_started(worker, e, bytes) }
          when :example_finished then example_finished(worker, rest[0], rest[1], bytes)
          when :message          then @sequencer.message(number, rest[0], bytes)
          when :deprecation      then @sequencer.deprecation(number, rest[0], bytes)
          when :custom           then @sequencer.custom(number, rest[0], rest[1], bytes)
          else handle_control_message(worker, kind, rest)
          end
        end

        def handle_control_message(worker, kind, rest)
          case kind
          when :unit_finished       then unit_finished(worker, *rest)
          when :non_example_failure then non_example_failure(*rest)
          when :exiting             then worker.exiting = true
          when :loaded              then loaded(worker, *rest)
          else report "#{worker.description} sent an unexpected #{kind.inspect} frame, which was ignored."
          end
        end

        def non_example_failure(payload, context_description)
          @configuration.reporter.notify_non_example_exception(RemoteException.rebuild(payload), context_description)
        end

        # Described as the worker that runs it described it.
        def group_started(number, group, descriptions, bytes)
          group.metadata.update(descriptions) if descriptions
          @sequencer.group_started(number, group, bytes)
        end

        def example_started(worker, example, bytes)
          worker.current_example = example
          @sequencer.example_started(worker.number, example, bytes)
        end

        def example_finished(worker, example_id, payload, bytes)
          index.with_example(example_id) do |example|
            worker.current_example = nil
            record_result(example, payload)
            @sequencer.example_finished(worker.number, example, bytes)
          end
        end

        def index
          @index ||= SuiteIndex.new(@world) { |warning| report warning }
        end

        # Recorded as soon as it arrives: failure counting and `.rspec_status`
        # persistence never wait for presentation.
        def record_result(example, payload)
          @results.record(example, payload)
          request_quit if fail_fast_limit_met?
        end

        def fail_fast_limit_met?
          limit = @configuration.fail_fast
          return false unless limit

          limit == true ? @results.failed_count >= 1 : @results.failed_count >= limit
        end

        def apply_result(example)
          @results.apply(example)
        end

        # A worker whose world wants to quit would only skip the groups it is
        # given (`ExampleGroup.run` returns at once), so it gets no more. The
        # events that should end the whole run reach the coordinator by other
        # means: an interrupt hits every process in the group, fail-fast is
        # counted here and suite hook failures are reported as such.
        def unit_finished(worker, index, passed, wants_to_quit, non_example_failure)
          @unit_states[index] = passed ? :passed : :failed if worker.current_unit == index
          worker.current_unit = nil
          @world.non_example_failure = true if non_example_failure
          return unless wants_to_quit && !@quitting && !@world.wants_to_quit

          report "#{worker.description} stopped after #{unit_description(index)} because `RSpec.world.wants_to_quit` was " \
                 "set while it ran (an interrupt, a suite hook failure or a spec setting it); it runs no further groups."
          stop(worker)
        end

        def dispatch_work
          @workers.each do |worker|
            if worker.loading
              # Nothing is left to give it once it has loaded.
              dismiss_worker(worker) if worker.state == :running && (@quitting || @queue.empty?)
            elsif worker.idle?
              @quitting || @queue.empty? ? stop(worker) : dispatch(worker, @queue.shift)
            end
          end
        end

        def dispatch(worker, index)
          worker.current_unit = index
          @unit_states[index] = :running
          return if write(worker, [:run, index, @units[index].id])

          @unit_states[index] = :pending
          @queue.unshift(index)
          worker.current_unit = nil
          worker.state = :stopping
        end

        def stop(worker)
          worker.state = :stopping
          worker.deadline = now + @configuration.parallel_shutdown_timeout + @watchdog_grace
          write(worker, [:stop])
        end

        # Busy workers are asked to stop between examples and, like workers
        # that have finished, get a deadline: a cancelled run ends even if a
        # worker is stuck inside an example.
        def request_quit
          return if @quitting

          @quitting = true
          @world.wants_to_quit = true
          @workers.each do |worker|
            next if worker.quit_sent || !worker.busy?

            worker.quit_sent = true
            worker.deadline = now + @configuration.parallel_shutdown_timeout
            write(worker, [:quit])
          end
        end

        # Control messages are a few bytes; a worker that is not reading them is
        # dead or wedged: reaping or the deadline set here deals with it, and
        # `judge_exit` still sees the state it was in. A worker that is merely
        # exiting gets the usual grace period to do so.
        def write(worker, message)
          worker.jobs.send(message)
          true
        rescue Errno::EPIPE, IOError
          worker.deadline ||= now + @configuration.parallel_shutdown_timeout + @watchdog_grace
          false
        end

        # `waitpid` for every worker on every wake-up of the loop -- once per
        # few frames when results stream in -- cost the coordinator a sixth of
        # its time on a large suite.
        def reap_workers_when_due
          return unless @reap_due || now >= @next_reap_at

          @reap_due = false
          @next_reap_at = now + REAP_INTERVAL
          reap_workers
        end

        def reap_workers
          @workers.each do |worker|
            next if worker.done? || worker.pid.nil?

            pid, status = worker.reap(@process)
            next unless pid

            receive_from(worker)
            worker.exit_status = status
            judge_exit(worker, status)
            worker.state = :exited
          end
        end

        def judge_exit(worker, status)
          case worker.state
          when :running
            # Asked to stop early, a worker may leave as soon as its group ends.
            return if status&.success? && worker.exiting && worker.current_unit.nil?

            lost(worker, "exited unexpectedly (#{WorkerHandle.describe_status(status)})")
          when :stopping
            return if status&.success?

            error("#{worker.description} failed while shutting down (#{WorkerHandle.describe_status(status)}). " \
                  "If its exit watchdog fired, its thread dump is on stderr above.")
          end
        end

        def enforce_deadlines
          @workers.each do |worker|
            next if worker.done? || worker.deadline.nil? || now < worker.deadline

            escalate(worker)
          end
        end

        def escalate(worker)
          case worker.escalation
          when nil
            overdue(worker)
            worker.state = :terminating
            worker.signal(@process, 'TERM', :term, now + @term_grace)
          when :term
            report "#{worker.description} is still alive #{Parallel.seconds(@term_grace)} after SIGTERM; sending SIGKILL."
            worker.signal(@process, 'KILL', :kill, now + @kill_grace)
          when :kill
            report "#{worker.description} is still alive #{Parallel.seconds(@kill_grace)} after SIGKILL; giving up on it."
            abort_unit(worker)
            worker.state = :abandoned
          end
        end

        def overdue(worker)
          timeout = Parallel.seconds(@configuration.parallel_shutdown_timeout)
          if worker.state == :stopping
            error("#{worker.description} did not exit within #{timeout} of finishing its last example group (its " \
                  "`after(:suite)` hooks or `at_exit` handlers may be stuck), so it was sent SIGTERM. Raise " \
                  "`config.parallel_shutdown_timeout` if it legitimately needs longer.")
          elsif worker.loading
            lost(worker, "took longer than #{Parallel.seconds(@load_allowance)}", SENT_TERM)
          elsif worker.quit_sent
            lost(worker, "did not stop within #{timeout} of being asked to stop early (fail-fast or interrupt)", SENT_TERM)
          else
            lost(worker, "stopped reporting for #{timeout}", SENT_TERM)
          end
        end

        def terminate(worker, reason)
          lost(worker, reason, SENT_TERM)
          worker.state = :terminating
          worker.signal(@process, 'TERM', :term, now + @term_grace)
        end

        # The worker can no longer be relied on. The example it was running,
        # if any, fails with the reason -- so the summary, `Failed examples:`
        # and `.rspec_status` point at it -- and otherwise the reason is
        # reported as an error outside of examples. Nothing it sends from now
        # on is read: it would report the example a second time.
        def lost(worker, what, consequence='')
          unit = worker.current_unit && unit_description(worker.current_unit)
          if worker.loading
            worker.loading = false
            error("#{worker.description} #{what} while loading the spec files#{consequence}.")
          elsif worker.current_example
            example, result = worker.lost_example_result(
              "#{worker.description} #{what} while running this example#{consequence}" \
              "#{"; the rest of #{unit} did not run" if unit}."
            )
            record_result(example, result)
            @sequencer.example_finished(worker.number, example, 0)
          else
            where = unit ? "while running #{unit}, outside of any example" : 'between example groups'
            error("#{worker.description} #{what} #{where}#{consequence}.")
          end
          abort_unit(worker)
          worker.results.close
        end

        # Counted as an error outside of examples, like a failing suite hook.
        def error(text)
          @configuration.reporter.notify_non_example_exception(WorkerError.new(text), 'An error occurred in a parallel worker.',
                                                               :failure_lines => [text])
        end

        def abort_unit(worker)
          if (index = worker.current_unit)
            @unit_states[index] = :aborted
            worker.current_unit = nil
          end
          @sequencer&.abandon(worker.number)
        end

        def report_unrun_units
          return if @quitting || @queue.empty?

          report "#{Formatters::Helpers.pluralize(@queue.size, 'example group')} did not run because no worker " \
                 "remained to run them."
        end

        def unit_description(index)
          group = @units[index]
          "#{group.description} (#{group.location})"
        end

        def report(message)
          @configuration.reporter.message(message)
        end

        def now
          Parallel.monotonic_time
        end
      end
    end
  end
end
