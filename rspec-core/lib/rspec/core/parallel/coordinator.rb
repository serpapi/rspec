RSpec::Support.require_rspec_core "parallel"
RSpec::Support.require_rspec_core "parallel/channel"
RSpec::Support.require_rspec_core "parallel/remote_exception"
RSpec::Support.require_rspec_core "parallel/worker_reporter"
RSpec::Support.require_rspec_core "parallel/worker"
RSpec::Support.require_rspec_core "parallel/sequencer"
RSpec::Support.require_rspec_core "parallel/results"
RSpec::Support.require_rspec_core "parallel/suite_index"
RSpec::Support.require_rspec_core "parallel/worker_handle"
RSpec::Support.require_rspec_core "parallel/worker_lifecycle"
RSpec::Support.require_rspec_core "parallel/worker_exits"

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
        include WorkerExits

        TICK = 0.25
        TERM_GRACE = 5.0
        KILL_GRACE = 5.0
        # Added to the shutdown timeout on the coordinator's side so that the
        # worker's own watchdog fires first and the thread dump comes from
        # the process that owns the threads.
        WATCHDOG_GRACE = 1.0
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
          @last_turn_at = nil
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
          loading = !@workers.empty?
          start_workers(@units) unless loading
          return run_serially if @workers.empty?

          passed = reporter.report(@world.example_count(@example_groups)) do
            @sequencer = Sequencer.new(reporter, :apply_result => method(:apply_result))

            begin
              @last_turn_at = now
              expect_loading_workers if loading
              event_loop
            ensure
              @winding_down = true
              terminate_remaining_workers
              @sequencer.finish
              @results.apply_all
              report_unrun_units
            end

            @unit_states.all? { |state| state == :passed }
          end

          # Terminated like a serial run would have been, once the workers are gone.
          raise SignalException, @termination_signal if @termination_signal
          # Ended by `exit` in a worker, as a serial run would have been,
          # unless one of them then had to be aborted.
          exit(@exit_status) if @exit_status && !@exit_spoiled

          @runner.exit_code(passed)
        ensure
          terminate_remaining_workers
          Parallel.restore_signals(@signal_handlers)
        end

      private

        # No worker could be started at all.
        def run_serially
          Parallel.restore_signals(@signal_handlers)
          @signal_handlers = {}
          @runner.run_specs(@example_groups)
        end

        def event_loop
          dispatch_work

          until @workers.all?(&:done?)
            return stop_for_signal if @termination_signal

            wait_for_results
            reap_workers_when_due
            request_quit if @world.wants_to_quit && !@quitting
            dispatch_work
            account_for_pauses
            enforce_deadlines
            @sequencer.tick
          end
        end

        # A run stopped from outside -- a CI job's timeout, say -- most often
        # was stuck in an example, so it says what each worker was running.
        def stop_for_signal
          @winding_down = true
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
          worker_started(worker) if worker.starting
          case kind
          when :group_started    then index.with_group(rest[0]) { |g| group_started(worker, g, rest[1], bytes) }
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
          when :exiting             then exiting(worker, *rest)
          when :loaded              then loaded(worker, *rest)
          when :expectations        then expectations(*rest)
          when :started             then nil
          else report "#{worker.description} sent an unexpected #{kind.inspect} frame, which was ignored."
          end
        end

        # Expectations counted in a worker, for a reporter that counts them
        # (`increment_expectation_count`, from an rspec-expectations that
        # reports every expectation to it).
        def expectations(count)
          reporter = @configuration.reporter
          return unless reporter.respond_to?(:increment_expectation_count)

          count.times { reporter.increment_expectation_count }
        end

        # Anything it sends shows it got past forking and runs RSpec's code.
        def worker_started(worker)
          worker.starting = false
          worker.clear_start_deadline
        end

        def non_example_failure(payload, context_description)
          @configuration.reporter.notify_non_example_exception(RemoteException.rebuild(payload), context_description)
        end

        # Described as the worker that runs it described it.
        def group_started(worker, group, descriptions, bytes)
          group.metadata.update(descriptions) if descriptions
          worker.last_started_group = group
          @sequencer.group_started(worker.number, group, bytes)
        end

        def example_started(worker, example, bytes)
          worker.current_example = example
          worker.started_example_ids[example.id] = true
          @sequencer.example_started(worker.number, example, bytes)
        end

        def example_finished(worker, example_id, payload, bytes)
          index.with_example(example_id) do |example|
            worker.current_example = nil
            record_result(example, payload)
            warn_unsent_metadata(worker, payload[:metadata])
            @sequencer.example_finished(worker.number, example, bytes)
          end
        end

        # Once per metadata key.
        def warn_unsent_metadata(worker, changes)
          unsent = changes && changes[:unsent]
          return unless unsent

          @unsent_metadata ||= {}
          unsent.each do |key, class_name|
            next if @unsent_metadata.key?(key)

            @unsent_metadata[key] = true
            RSpec.warning "#{worker.description} set example metadata #{key} to a #{class_name}, which cannot be " \
                          "marshaled to reach the formatters, so they see the value it had before the example ran. " \
                          "Later values of #{key} that cannot be marshaled are not reported", :call_site => nil
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

        def fail_fast_limit_met?(failed_count=@results.failed_count)
          limit = @configuration.fail_fast
          return false unless limit

          limit == true ? failed_count >= 1 : failed_count >= limit
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

        # A worker that runs out of work -- or is still loading -- waits while
        # another one is busy: if that one is lost, what it had not started
        # goes back to the queue.
        def dispatch_work
          busy = @workers.count(&:busy?)
          @workers.each do |worker|
            if worker.loading
              # Nothing is left to give it once it has loaded.
              dismiss_worker(worker) if worker.state == :running && nothing_left?(busy)
            elsif worker.idle?
              if !@quitting && !@queue.empty?
                dispatch(worker, @queue.shift)
                busy += 1 if worker.busy?
              elsif nothing_left?(busy)
                stop(worker)
              end
            end
          end
        end

        # Nor will be: no busy worker is left to hand any back.
        def nothing_left?(busy)
          @quitting || (@queue.empty? && busy.zero?)
        end

        def dispatch(worker, index)
          worker.current_unit = index
          worker.started_example_ids = {}
          worker.last_started_group = nil
          @unit_states[index] = :running
          unit = @units[index]
          return if write(worker, [:run, index, unit.group.id, unit.example_ids])

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

        def enforce_deadlines
          @workers.each do |worker|
            next if worker.done? || worker.deadline.nil? || now < worker.deadline

            escalate(worker)
          end
        end

        def escalate(worker)
          case worker.escalation
          when nil
            spoil_exit
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

        # Counted as an error outside of examples, like a failing suite hook.
        def error(text)
          spoil_exit
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

        def unit_description(index)
          @units[index].description
        end

        def pluralize(count, noun)
          Formatters::Helpers.pluralize(count, noun)
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
