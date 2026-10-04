module RSpec
  module Core
    module Parallel
      # The coordinating side of a parallel run. It forks the workers after the
      # suite has been loaded (so they inherit every example group, filter and
      # configuration), hands out top-level example groups from a shared queue,
      # streams the results onto the configuration's reporter -- the one and
      # only place formatters run -- and makes sure that no worker, however it
      # misbehaves, can keep the run from finishing.
      # @private
      class Coordinator
        TICK = 0.25
        TERM_GRACE = 5.0
        KILL_GRACE = 5.0
        # Added to the shutdown timeout on the coordinator's side so that the
        # worker's own watchdog fires first and the thread dump comes from
        # the process that owns the threads.
        WATCHDOG_GRACE = 1.0

        def initialize(runner, example_groups, units, worker_count, options={})
          @runner = runner
          @configuration = runner.configuration
          @world = runner.world
          @example_groups = example_groups
          @units = units
          @worker_count = worker_count
          @process = options.fetch(:process) { Process }
          @spawn = options.fetch(:spawn) { method(:fork_worker) }
          @tick = options.fetch(:tick, TICK)
          @term_grace = options.fetch(:term_grace, TERM_GRACE)
          @kill_grace = options.fetch(:kill_grace, KILL_GRACE)
          @watchdog_grace = options.fetch(:watchdog_grace, WATCHDOG_GRACE)
          @workers = []
          @queue = (0...@units.size).to_a
          @unit_states = Array.new(@units.size, :pending)
          @results = Results.new
          @quitting = false
          @unknown_ids = []
        end

        # @return [Integer] the exit code for the run
        def run
          reporter = @configuration.reporter

          passed = reporter.report(@world.example_count(@example_groups)) do
            @sequencer = Sequencer.new(reporter, :apply_result => method(:apply_result))

            begin
              start_workers
              event_loop
            ensure
              terminate_remaining_workers
              @sequencer.finish
              @results.apply_all
              report_unrun_units
            end

            passed?
          end

          @runner.exit_code(passed)
        end

        # @private
        def passed?
          @unit_states.all? { |state| state == :passed }
        end

      private

        def start_workers
          flush_output
          @process.warmup if @process.respond_to?(:warmup)
          @worker_count.times { |index| @workers << WorkerHandle.new(index + 1) }

          @workers.each do |worker|
            worker.pid = @spawn.call(worker, @workers)
            worker.close_child_ends
          end
        end

        # Whatever the formatters wrote before `start` (seed, run options) is
        # still buffered in streams that are not `$stdout`; forked workers
        # would write it again when they exit.
        def flush_output
          streams = @configuration.formatters.map { |formatter| formatter.output if formatter.respond_to?(:output) }
          streams.push(@configuration.output_stream, @configuration.error_stream, @configuration.deprecation_stream)
          streams.push($stdout, $stderr)
          streams.each { |stream| stream.flush if stream.respond_to?(:flush) }
        end

        def fork_worker(worker, workers)
          @process.fork do
            # :nocov: - runs in the forked worker process, covered by integration specs
            workers.each { |other| other.close_all unless other.equal?(worker) }
            worker.close_parent_ends
            Worker.new(worker.number, @units, worker.child_jobs, worker.child_results).run
            # :nocov:
          end
        end

        def event_loop
          dispatch_work

          until @workers.all?(&:done?)
            wait_for_results
            reap_workers
            request_quit if @world.wants_to_quit && !@quitting
            dispatch_work
            enforce_deadlines
            @sequencer.tick
          end
        end

        def wait_for_results
          readers = @workers.reject { |worker| worker.done? || worker.results.eof? || worker.results.closed? }

          if readers.empty?
            sleep @tick
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

          # A worker whose results pipe closed while it was still running is
          # either about to be reaped or wedged; bound the wait for it.
          worker.deadline ||= now + @configuration.parallel_shutdown_timeout + @watchdog_grace if worker.results.eof?
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
          case kind
          when :group_started    then with_group(rest[0]) { |g| @sequencer.group_started(worker.number, g, bytes) }
          when :group_finished   then with_group(rest[0]) { |g| @sequencer.group_finished(worker.number, g, rest[1], bytes) }
          when :example_started  then with_example(rest[0]) { |e| @sequencer.example_started(worker.number, e, bytes) }
          when :example_finished then example_finished(worker, rest[0], rest[1], bytes)
          when :message          then @sequencer.message(worker.number, rest[0], bytes)
          when :deprecation      then @sequencer.deprecation(worker.number, rest[0], bytes)
          when :custom           then @sequencer.custom(worker.number, rest[0], rest[1], bytes)
          else handle_control_message(worker, kind, rest)
          end
        end

        def handle_control_message(worker, kind, rest)
          case kind
          when :unit_finished       then unit_finished(worker, *rest)
          when :non_example_failure then non_example_failure(*rest)
          when :exiting             then worker.exiting = true
          else report "#{worker.description} sent an unexpected #{kind.inspect} frame, which was ignored."
          end
        end

        def non_example_failure(payload, context_description)
          @configuration.reporter.notify_non_example_exception(RemoteException.rebuild(payload), context_description)
        end

        def example_finished(worker, example_id, payload, bytes)
          with_example(example_id) do |example|
            record_result(example, payload)
            @sequencer.example_finished(worker.number, example, bytes)
          end
        end

        def with_example(id, &block)
          with_known(index.example(id), id, &block)
        end

        def with_group(id, &block)
          with_known(index.group(id), id, &block)
        end

        def index
          @index ||= SuiteIndex.new(@world)
        end

        # Examples and groups defined while running (rather than loading) the
        # suite exist only in the worker that defined them.
        def with_known(target, id)
          return yield(target) if target
          return if @unknown_ids.include?(id)

          @unknown_ids << id
          report "A parallel worker reported results for #{id}, which was not defined when the suite was loaded; " \
                 "examples and groups defined at run time cannot be reported by `--parallel`."
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
            next unless worker.idle?

            if @quitting || @queue.empty?
              stop(worker)
            else
              dispatch(worker, @queue.shift)
            end
          end
        end

        def dispatch(worker, index)
          worker.current_unit = index
          @unit_states[index] = :running
          return if write(worker, [:run, index])

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
        # `judge_exit` still sees the state it was in.
        def write(worker, message)
          worker.jobs.send(message)
          true
        rescue Errno::EPIPE, IOError
          worker.deadline ||= now
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

        def judge_exit(worker, status)
          case worker.state
          when :running
            where = worker.current_unit ? "while running #{unit_description(worker.current_unit)}" : "between example groups"
            report "#{worker.description} exited unexpectedly (#{WorkerHandle.describe_status(status)}) #{where}; " \
                   "the examples it had not finished did not run."
            @world.non_example_failure = true
            abort_unit(worker)
          when :stopping
            return if status&.success?

            report "#{worker.description} failed while shutting down (#{WorkerHandle.describe_status(status)}). " \
                   "If its exit watchdog fired, its thread dump is on stderr above."
            @world.non_example_failure = true
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
            report "#{worker.description} #{overdue_description(worker)}; sending SIGTERM. Raise " \
                   "`config.parallel_shutdown_timeout` if it legitimately needs longer."
            @world.non_example_failure = true
            abort_unit(worker)
            worker.state = :terminating
            signal(worker, 'TERM', :term, @term_grace)
          when :term
            report "#{worker.description} is still alive #{Parallel.seconds(@term_grace)} after SIGTERM; sending SIGKILL."
            signal(worker, 'KILL', :kill, @kill_grace)
          when :kill
            report "#{worker.description} is still alive #{Parallel.seconds(@kill_grace)} after SIGKILL; giving up on it."
            abort_unit(worker)
            worker.state = :abandoned
          end
        end

        def overdue_description(worker)
          timeout = Parallel.seconds(@configuration.parallel_shutdown_timeout)
          if worker.state == :stopping
            "did not exit within #{timeout} of finishing its last example group (its `after(:suite)` hooks or " \
              "`at_exit` handlers may be stuck)"
          elsif worker.quit_sent
            "did not stop within #{timeout} of being asked to stop early (fail-fast or interrupt)"
          else
            "stopped reporting #{timeout} ago"
          end
        end

        def terminate(worker, reason)
          report "#{worker.description} #{reason}; sending SIGTERM."
          @world.non_example_failure = true
          abort_unit(worker)
          worker.state = :terminating
          signal(worker, 'TERM', :term, @term_grace)
        end

        def signal(worker, name, escalation, grace)
          worker.escalation = escalation
          worker.deadline = now + grace
          worker.kill(@process, name)
        end

        def abort_unit(worker)
          if (index = worker.current_unit)
            @unit_states[index] = :aborted
            worker.current_unit = nil
          end
          @sequencer.abandon(worker.number)
        end

        # Runs on every exit path, so an exception in the coordinator can never
        # leave workers behind. Bounded by the same TERM/KILL grace periods.
        def terminate_remaining_workers
          @workers.each { |worker| worker.state = :exited if worker.pid.nil? }
          live = @workers.reject(&:done?)
          return if live.empty?

          live.each do |worker|
            worker.state = :terminating
            signal(worker, 'TERM', :term, @term_grace)
          end
          wait_for_exit(live, @term_grace)
          live.reject(&:done?).each { |worker| signal(worker, 'KILL', :kill, @kill_grace) }
          wait_for_exit(live, @kill_grace)

          live.reject(&:done?).each do |worker|
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
