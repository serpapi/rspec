module RSpec
  module Core
    module Parallel
      # Runs in a forked worker process: executes the example groups the
      # coordinator dispatches over the jobs channel and streams results back
      # over the results channel. Suite hooks run once per worker, around all
      # the groups it executes; a worker given none exits without running
      # them. A worker forked before the spec files were loaded (no
      # `example_groups`) loads them itself and reports what it loaded first.
      #
      # A worker that has finished its work but does not exit (a non-daemon
      # thread in `at_exit`, a browser that will not quit, ...) is aborted by
      # its own watchdog thread after `parallel_shutdown_timeout` seconds,
      # with a dump of every thread's backtrace on stderr, because the process
      # holding the stacks is the only one that can print them. The coordinator
      # independently bounds the wait from its side.
      # @private
      class Worker
        # Seconds between checks that the coordinating process is still there.
        COORDINATOR_POLL = 0.5

        attr_reader :number

        def initialize(number, example_groups, jobs, results, options={})
          @number = number
          @example_groups = example_groups
          @jobs = jobs
          @results = results
          @configuration = options.fetch(:configuration) { RSpec.configuration }
          @world = options.fetch(:world) { RSpec.world }
          @stderr = options[:stderr]
          @exit = options.fetch(:exit) { lambda { |status| exit(status) } }
          @exit_now = options.fetch(:exit_now) { lambda { |status| exit!(status) } }
          @trap = options.fetch(:trap) do
            lambda { |signal, command=nil, &handler| command ? trap(signal, command) : trap(signal, &handler) }
          end
          @ppid = options.fetch(:ppid) { Process.method(:ppid) }
          @watch_coordinator = options.fetch(:watch_coordinator, true)
          @signal_handlers = options.fetch(:signal_handlers, {})
        end

        # @return [String] the `TEST_ENV_NUMBER` value for this worker, following
        #   the convention established by parallel_tests: `""` for the first
        #   worker, then `"2"`, `"3"`, ...
        def test_env_number
          @number == 1 ? "" : @number.to_s
        end

        # Never returns: ends the process when the coordinator has no more work.
        def run
          watch_coordinator if @watch_coordinator
          prepare
          load_spec_files unless @example_groups

          # One that ran nothing has no suite hooks or `at_exit` handlers to run.
          run_units ? @exit.call(0) : @exit_now.call(0)
        rescue Errno::EPIPE, IOError
          # The coordinator is gone; there is nobody left to report to.
          @exit_now.call(1)
        end

        # A worker notices that the coordinator is gone when it next reports
        # to it -- which a worker stuck in a long example does not do. Its
        # parent changes when the coordinator dies, so it does not outlive the
        # coordinator by more than {COORDINATOR_POLL} seconds.
        # @private
        def watch_coordinator
          coordinator = @ppid.call
          thread = Thread.new do
            sleep COORDINATOR_POLL while @ppid.call == coordinator
            begin
              (@stderr || $stderr).puts "\nRSpec parallel worker #{@number} (pid #{Process.pid}) is exiting because the " \
                                        "coordinating process (pid #{coordinator}) is gone."
            rescue IOError, SystemCallError
              nil
            end
            @exit_now.call(1)
          end
          thread.report_on_exception = false
          thread
        end

        # @private
        def prepare
          Runner.disable_autorun!
          trap_interrupt
          # Inherited from the coordinator: its handlers, which a worker swaps
          # back for the ones they replaced.
          @signal_handlers.each { |signal, previous| @trap.call(signal, previous) }
          ENV['TEST_ENV_NUMBER'] = test_env_number
          @configuration.parallel_worker_number = @number
          @configuration.reset_reporter
          @reporter = WorkerReporter.new(@configuration, @results, @jobs, @world)
          @configuration.reporter = @reporter
        end

        # Same contract as `Runner.handle_interrupt` -- stop between examples,
        # force-quit on a second interrupt -- without the message, which the
        # coordinator prints once for the run.
        # @private
        def trap_interrupt
          world = @world
          exit_now = @exit_now
          @trap.call('INT') do
            if world.wants_to_quit
              exit_now.call(1)
            else
              world.wants_to_quit = true
            end
          end
        end

        # Loads the spec files as the coordinator does after forking, and
        # tells it which examples that defined and the first load error, if
        # any: it uses the worker only if its own loading went the same way.
        # What loading reports is for the coordinator to report.
        # @private
        def load_spec_files
          forwarding = @configuration.reporter
          @configuration.reporter = loading = LoadReporter.new(@configuration)
          @configuration.load_spec_files
          @configuration.reporter = forwarding
          @groups_by_id = @world.example_groups.to_h { |group| [group.id, group] }
          @results.send([:loaded, Parallel.suite_digest(@world), loading.failure])
        end

        # @return [Boolean] whether it was given anything to run
        # @private
        def run_units
          message = @jobs.receive
          return false unless message && message.first == :run

          @configuration.with_suite_hooks do
            while message && message.first == :run
              run_unit(message[1], message[2])
              message = @jobs.receive
            end

            # Armed before `after(:suite)` hooks so that a hook that never
            # returns is bounded too.
            arm_watchdog
          end

          @results.send([:exiting])
          true
        end

        # @private
        def run_unit(index, id)
          group = @groups_by_id ? @groups_by_id.fetch(id) : @example_groups.fetch(index)
          passed = group.run(@reporter)
          @results.send([:unit_finished, index, passed ? true : false,
                         !!@world.wants_to_quit, !!@world.non_example_failure])
        end

        # @private
        def arm_watchdog
          timeout = @configuration.parallel_shutdown_timeout
          thread = Thread.new do
            Parallel.sleep_running(timeout)
            begin
              dump_threads(timeout)
            rescue IOError, SystemCallError
              nil # stderr was closed or replaced; aborting matters more than the dump
            end
            @exit_now.call(1)
          end
          thread.report_on_exception = false
          thread
        end

        # @private
        def dump_threads(timeout)
          stderr = @stderr || $stderr
          stderr.puts "\nRSpec parallel worker #{@number} (pid #{Process.pid}) did not exit within #{timeout} " \
                      "seconds of finishing its last example group; aborting it. Threads at this point:"
          Thread.list.each do |thread|
            stderr.puts "  #{thread.inspect}#{' (this watchdog)' if thread.equal?(Thread.current)}"
            Array(thread.backtrace).each { |line| stderr.puts "    #{line}" }
          end
          stderr.flush
        end

        # Reports nothing: while a worker loads the spec files, what they
        # report is for the coordinating process, which loads them too, to
        # report. Remembers the first error.
        # @private
        class LoadReporter < Reporter
          attr_reader :failure

          def notify_non_example_exception(exception, context_description, _presenter_options={})
            return if @failure

            @failure = "#{context_description} #{exception.class}: #{exception.message}"
          end
        end
      end
    end
  end
end
