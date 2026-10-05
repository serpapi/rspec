module RSpec::Core::Parallel
  # Stands in for `Process` so that workers can be plain threads: `kill`
  # ends the thread unless the "signal" is one the fake worker is said to
  # survive, and `waitpid2` reports a status once the thread is dead.
  class FakeProcess
    Status = Struct.new(:exitstatus, :termsig) do
      def success?
        termsig.nil? && exitstatus == 0
      end

      def signaled?
        !termsig.nil?
      end
    end

    attr_reader :signals, :warmups

    def initialize(survives=[])
      @threads = {}
      @statuses = {}
      @signals = []
      @survives = survives
      @warmups = 0
      @next_pid = 40_000
    end

    def register(thread, &exit_status)
      pid = (@next_pid += 1)
      @threads[pid] = thread
      @statuses[pid] = exit_status
      pid
    end

    def warmup
      @warmups += 1
    end

    def waitpid2(pid, _flags)
      thread = @threads.fetch(pid)
      return nil if thread.alive?

      [pid, @statuses[pid].call || Status.new(1, nil)]
    end

    def kill(name, pid)
      @signals << [name, pid]
      return if @survives.include?(name)

      @statuses[pid] = lambda { Status.new(nil, Signal.list[name]) }
      @threads.fetch(pid).kill
    end
  end

  RSpec.describe Coordinator do
    let(:config) { RSpec.configuration }
    let(:world) { RSpec.world }
    let(:output) { StringIO.new }
    let(:runner) { RSpec::Core::Runner.new(RSpec::Core::ConfigurationOptions.new([]), config, world) }
    let(:process) { FakeProcess.new }
    let(:worker_stderr) { StringIO.new }
    let(:worker_exits) { {} }
    let(:worker_behaviours) { {} }

    around { |example| without_env_vars("TEST_ENV_NUMBER", &example) }

    before do
      config.output_stream = output
      config.add_formatter("progress", output)
    end

    # Runs a real Worker on a thread, with its own configuration object so
    # that it cannot replace the coordinator's reporter the way a forked
    # worker replaces its inherited copy, and with its own copies of the
    # pipe ends since the coordinator closes the child's ends after spawning
    # (as it must after a fork).
    def spawn(handle, _handles)
      behaviour = worker_behaviours.fetch(handle.number, :normal)
      jobs = Channel::Reader.new(handle.child_jobs.io.dup)
      results = Channel::Writer.new(handle.child_results.io.dup)
      exit_status = nil

      thread = Thread.new do
        Thread.current.report_on_exception = false
        case behaviour
        when :garbage        then stall_after { results.io.write("garbage\n") }
        when :partial_frame  then stall_after { results.io.write("999\nabc") }
        when :closes_results then stall_after { results.close }
        else
          send_unknown_ids(results) if behaviour == :unknown_ids
          run_fake_worker(handle.number, jobs, results, behaviour) { |status| exit_status = status }
        end
      end

      @process.register(thread) { exit_status && FakeProcess::Status.new(exit_status, nil) }
    end

    def stall_after
      yield
      sleep
    end

    def send_unknown_ids(results)
      results.send([:example_started, "./nope_spec.rb[1:1]"])
      results.send([:example_started, "./nope_spec.rb[1:1]"])
      results.send([:group_started, "./nope_spec.rb[1]"])
      results.send([:weird])
    end

    def run_fake_worker(number, jobs, results, behaviour, &record_exit)
      worker_config = RSpec::Core::Configuration.new
      worker_config.parallel_shutdown_timeout = 60
      Thread.current[:fake_worker_world] = RSpec::Core::World.new(worker_config)
      Worker.new(number, @units, jobs, results,
                 :configuration => worker_config,
                 :world => Thread.current[:fake_worker_world],
                 :stderr => worker_stderr,
                 :exit => lambda { |status|
                   record_exit.call(status)
                   sleep if behaviour == :never_exits
                 },
                 :exit_now => record_exit,
                 :trap => lambda { |*| },
                 :watch_coordinator => false).run
    end

    def coordinator(units, worker_count=2, options={})
      @units = units
      @process = options.fetch(:process, process)
      Coordinator.new(runner, units, units, worker_count,
                      { :process => @process, :spawn => method(:spawn), :tick => 0.01,
                        :term_grace => 0.1, :kill_grace => 0.1, :watchdog_grace => 0.05 }.merge(options))
    end

    def passing_group(name)
      RSpec.describe(name) { it("passes") {} }
    end

    def failing_group(name)
      RSpec.describe(name) { it("fails") { raise "boom" } }
    end

    def messages
      output.string
    end

    it "runs every group across the workers, reports through the configured formatters and exits 0 when all pass" do
      groups = %w[A B C D E].map { |name| passing_group(name) }

      exit_code = coordinator(groups, 3).run

      expect(exit_code).to eq(0)
      expect(messages).to include("5 examples, 0 failures")
      expect(messages).to match(/^\.\.\.\.\.$/)
      expect(process.signals).to be_empty
      expect(process.warmups).to eq(1)
      expect(groups.flat_map(&:examples).map { |e| e.execution_result.status }).to all(eq(:passed))
    end

    it "returns the failure exit code when an example fails" do
      exit_code = coordinator([passing_group("A"), failing_group("B")]).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(world.non_example_failure).to be_falsey
      expect(messages).to include("2 examples, 1 failure", "boom")
    end

    it "persists results on the real examples as they arrive so statuses survive even if replay is cut short" do
      groups = [passing_group("A"), failing_group("B")]
      coordinator(groups).run

      expect(groups[0].examples.first.execution_result.status).to eq(:passed)
      expect(groups[1].examples.first.execution_result.status).to eq(:failed)
      expect(groups[1].examples.first.exception.message).to eq("boom")
      expect(groups[1].examples.first.exception).to be_a(RuntimeError)
    end

    it "honours the configured exit codes" do
      config.failure_exit_code = 7
      expect(coordinator([passing_group("A"), failing_group("B")]).run).to eq(7)
    end

    describe "fail-fast" do
      it "stops dispatching and asks busy workers to stop as soon as the limit is met, reporting a failure exit code" do
        config.fail_fast = true
        slow = RSpec.describe("Slow") do
          it("waits") { sleep 0.3 }
          it("never runs") {}
        end
        groups = [failing_group("Fails first"), slow, passing_group("Never dispatched")]

        exit_code = coordinator(groups, 2).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(groups.last.examples.first.execution_result.status).to be_nil
        expect(world.wants_to_quit).to be true
      end

      it "accepts a worker leaving as soon as its group ends once it was asked to stop" do
        config.fail_fast = true
        config.error_exit_code = 5
        # Holds the coordinator up so that the worker has finished its group
        # before the failure is handled and has exited before that pass ends.
        slow_formatter = Class.new do
          RSpec::Core::Formatters.register self, :example_started, :example_failed

          def initialize(_output); end

          def example_started(_notification)
            sleep 0.1
          end

          def example_failed(_notification)
            sleep 0.2
          end
        end
        config.add_formatter(slow_formatter, output)
        groups = [failing_group("Fails"), passing_group("Never dispatched")]

        exit_code = coordinator(groups, 1).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(messages).to include("1 example, 1 failure")
        expect(messages).not_to include("exited unexpectedly", "outside of examples")
      end
    end

    describe "a worker that exits while running a group" do
      it "fails the example it was running with the reason and leaves the rest of the group unknown" do
        config.error_exit_code = 5
        crashing = RSpec.describe("Crashing") do
          it("finishes") {}
          it("dies") { Thread.current.kill }
          it("never runs") {}
        end
        groups = [crashing, passing_group("B")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        dies = crashing.examples[1]
        expect(crashing.examples.map { |e| e.execution_result.status }).to eq([:passed, :failed, nil])
        expect(dies.exception).to be_a(WorkerError)
        expect(dies.exception.message).to match(
          /\AParallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 1\) while running this example; the rest of Crashing \(.+\) did not run\.\z/
        )
        expect(dies.exception.backtrace).to eq([dies.location])
        expect(dies.execution_result.run_time).to be >= 0
        expect(messages).to include("Crashing dies", "3 examples, 1 failure", "rspec #{dies.location_rerun_argument}")
      end

      it "reports an error outside of examples when it was not running one" do
        config.error_exit_code = 5
        crashing = RSpec.describe("Crashing") do
          before(:context) { Thread.current.kill }
          it("never runs") {}
        end
        groups = [crashing, passing_group("B")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(5)
        expect(crashing.examples.first.execution_result.status).to be_nil
        expect(messages).to match(
          /An error occurred in a parallel worker\.\nParallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 1\) while running Crashing \(.+\), outside of any example\./
        )
        expect(messages).to include("1 example, 0 failures, 1 error occurred outside of examples")
      end
    end

    describe "a worker that does not exit after finishing" do
      before { config.parallel_shutdown_timeout = 0.05 }

      it "is sent SIGTERM after the shutdown timeout and the run fails with the error exit code" do
        worker_behaviours[2] = :never_exits
        config.error_exit_code = 3

        exit_code = coordinator([passing_group("A"), passing_group("B")]).run

        expect(exit_code).to eq(3)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(messages).to include("did not exit within 0.05 seconds of finishing its last example group", "so it was sent SIGTERM")
        expect(messages).to include("2 examples, 0 failures, 1 error occurred outside of examples")
      end

      it "is sent SIGKILL when it survives SIGTERM" do
        worker_behaviours[2] = :never_exits
        process = FakeProcess.new(["TERM"])

        exit_code = coordinator([passing_group("A"), passing_group("B")], 2, :process => process).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(%w[TERM KILL])
        expect(messages).to include("still alive 0.1 seconds after SIGTERM; sending SIGKILL")
      end

      it "is given up on, so the run still ends, when it survives SIGKILL" do
        worker_behaviours[2] = :never_exits
        process = FakeProcess.new(%w[TERM KILL])
        started = RSpec::Core::Parallel.monotonic_time

        exit_code = coordinator([passing_group("A"), passing_group("B")], 2, :process => process).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(%w[TERM KILL])
        expect(messages).to include("still alive 0.1 seconds after SIGKILL; giving up on it")
        expect(RSpec::Core::Parallel.monotonic_time - started).to be < 5
      end
    end

    describe "a worker that is asked to stop early but does not" do
      it "is terminated after the shutdown timeout so a cancelled run still ends" do
        config.parallel_shutdown_timeout = 0.1
        config.fail_fast = true
        worker_behaviours[2] = :partial_frame
        groups = [failing_group("Fails"), passing_group("Held by the stalled worker")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(messages).to include("did not stop within 0.1 seconds of being asked to stop early")
        expect(messages).to include("1 example, 1 failure")
      end

      # Fake workers share `RSpec.world`: a group started after fail-fast set
      # `wants_to_quit` would be skipped, so the failure waits for it to start.
      def failure_once_running(name, &body)
        started = Queue.new
        running = RSpec.describe(name) do
          it("runs") do
            started << true
            instance_exec(&body)
          end
        end
        fails = RSpec.describe("Fails") do
          it("fails once #{name} runs") do
            deadline = RSpec::Core::Parallel.monotonic_time + 10
            sleep 0.01 while started.empty? && RSpec::Core::Parallel.monotonic_time < deadline
            raise "boom"
          end
        end
        [fails, running]
      end

      it "fails the example it is stuck in with the reason" do
        config.parallel_shutdown_timeout = 0.1
        config.fail_fast = true
        groups = failure_once_running("Stuck") { sleep }
        stuck = groups.last

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(stuck.examples.first.exception.message).to match(
          /\AParallel worker 2 \(pid \d+\) did not stop within 0\.1 seconds of being asked to stop early \(fail-fast or interrupt\) while running this example, so it was sent SIGTERM; the rest of Stuck \(.+\) did not run\.\z/
        )
        expect(messages).to include("2 examples, 2 failures")
      end

      it "ignores what it sends once it was given up on, so that its example is reported once" do
        config.parallel_shutdown_timeout = 0.1
        config.fail_fast = true
        groups = failure_once_running("Late") { sleep 0.5 }

        exit_code = coordinator(groups, 2, :process => FakeProcess.new(["TERM"]), :term_grace => 5).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(groups.last.examples.first.exception).to be_a(WorkerError)
        expect(messages).to include("2 examples, 2 failures")
        expect(messages).not_to include("sending SIGKILL")
      end
    end

    it "terminates a worker that sends data it cannot decode" do
      worker_behaviours[2] = :garbage

      exit_code = coordinator([passing_group("A"), passing_group("B")]).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(process.signals.map(&:first)).to eq(["TERM"])
      expect(messages).to include("sent data the coordinator could not decode (RSpec::Core::Parallel::Channel::ProtocolError")
    end

    it "warns once about ids it does not know and about unexpected frames" do
      worker_behaviours[2] = :unknown_ids

      exit_code = coordinator([passing_group("A"), passing_group("B")]).run

      expect(exit_code).to eq(0)
      expect(messages.scan("./nope_spec.rb[1:1], which was not defined when the suite was loaded").size).to eq(1)
      expect(messages.scan("./nope_spec.rb[1], which was not defined").size).to eq(1)
      expect(messages).to include("sent an unexpected :weird frame")
      expect(messages).to include("2 examples, 0 failures")
    end

    it "reports the groups that never ran when no worker remains" do
      crashing = RSpec.describe("Crashing") { it("dies") { Thread.current.kill } }
      groups = [crashing, RSpec.describe("Also crashing") { it("dies") { Thread.current.kill } }, passing_group("C"), passing_group("D")]
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

      exit_code = coordinator(groups).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(messages).to include("2 example groups did not run because no worker remained to run them")
    end

    it "stops dispatching when the world wants to quit, as on Ctrl-C" do
      groups = [RSpec.describe("Interrupting") { it("interrupts") { RSpec.world.wants_to_quit = true } }, passing_group("B"), passing_group("C")]
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

      exit_code = coordinator(groups, 1).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(groups.last.examples.first.execution_result.status).to be_nil
    end

    it "reports a suite hook failure in a worker as an error of the run, counted in the summary" do
      failing = RSpec.describe("Reports a suite failure") do
        it("fails the suite") do
          error = RuntimeError.new("after suite broke")
          error.set_backtrace(["./spec/support/hooks.rb:7"])
          RSpec.configuration.reporter.notify_non_example_exception(error, "An error occurred in an `after(:suite)` hook.")
        end
      end
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
      config.error_exit_code = 9

      exit_code = coordinator([failing, passing_group("B")], 1).run

      expect(exit_code).to eq(9)
      expect(world.non_example_failure).to be true
      expect(messages).to include("An error occurred in an `after(:suite)` hook.", "after suite broke", "./spec/support/hooks.rb:7")
      expect(messages).to include("2 examples, 0 failures, 1 error occurred outside of examples")
    end

    it "lets a formatter's own exception surface instead of blaming the worker" do
      broken = Class.new do
        RSpec::Core::Formatters.register self, :example_passed
        def initialize(_output); end

        def example_passed(_notification)
          raise ArgumentError, "formatter bug"
        end
      end
      config.add_formatter(broken, output)

      expect { coordinator([passing_group("A"), passing_group("B")]).run }.to raise_error(ArgumentError, "formatter bug")
      expect(messages).not_to include("could not decode")
    end

    it "flushes the formatters' and configuration's streams before forking so workers do not replay buffered output" do
      file_output = StringIO.new
      allow(file_output).to receive(:flush).and_call_original
      config.add_formatter(RSpec::Core::Formatters::ProgressFormatter.new(file_output))

      coordinator([passing_group("A"), passing_group("B")]).run

      expect(file_output).to have_received(:flush).at_least(:once)
      expect(file_output.string).to include("2 examples, 0 failures")
    end

    it "copes with workers that could not be spawned when the coordinator fails" do
      failing_spawn = lambda do |handle, handles|
        raise Errno::EAGAIN, "fork" if handle.number == 2

        spawn(handle, handles)
      end

      expect { coordinator([passing_group("A"), passing_group("B")], 2, :spawn => failing_spawn).run }.to raise_error(Errno::EAGAIN)
      expect(process.signals.map(&:first)).to eq(["TERM"])
    end

    it "treats a non-example failure reported by a worker as one of the run" do
      failing = RSpec.describe("Reports a suite failure") do
        it("fails the suite") { RSpec.configuration.reporter.notify_non_example_exception(RuntimeError.new("suite"), "in a hook") }
      end
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
      config.error_exit_code = 9

      exit_code = coordinator([failing, passing_group("B"), passing_group("C")], 1).run

      expect(exit_code).to eq(9)
      expect(world.non_example_failure).to be true
      expect(messages).to include("3 examples, 0 failures, 1 error occurred outside of examples")
    end

    it "stops giving groups to a worker whose own world wants to quit and lets the others finish the queue" do
      quitting = RSpec.describe("Quits") do
        it("sets the flag on the worker's world") { Thread.current[:fake_worker_world].wants_to_quit = true }
      end
      groups = [quitting] + %w[B C D E].map { |name| passing_group(name) }
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

      exit_code = coordinator(groups, 2).run

      expect(exit_code).to eq(0)
      expect(messages).to match(/Parallel worker \d \(pid \d+\) stopped after Quits .* because `RSpec.world.wants_to_quit` was set/)
      expect(messages).to include("5 examples, 0 failures")
      expect(groups.drop(1).flat_map(&:examples).map { |e| e.execution_result.status }).to all(eq(:passed))
    end

    it "requeues a group when its worker cannot be reached and lets another worker run it" do
      config.parallel_shutdown_timeout = 0.2
      spawn_with_closed_jobs = lambda do |handle, handles|
        pid = spawn(handle, handles)
        handle.jobs.io.close_write if handle.number == 2
        pid
      end
      groups = [passing_group("A"), passing_group("B"), passing_group("C")]

      exit_code = coordinator(groups, 2, :spawn => spawn_with_closed_jobs).run

      expect(exit_code).to eq(0)
      expect(messages).to include("3 examples, 0 failures")
    end

    it "terminates the workers that are still running when the coordinator itself fails" do
      worker_behaviours[1] = :partial_frame
      worker_behaviours[2] = :partial_frame
      instance = coordinator([passing_group("A"), passing_group("B")])
      allow(instance).to receive(:dispatch_work).and_raise("coordinator bug")

      expect { instance.run }.to raise_error("coordinator bug")
      expect(process.signals.map(&:first)).to eq(%w[TERM TERM])
    end

    it "gives up on live workers that survive both signals when the coordinator itself fails" do
      worker_behaviours[1] = :partial_frame
      worker_behaviours[2] = :partial_frame
      process = FakeProcess.new(%w[TERM KILL])
      instance = coordinator([passing_group("A"), passing_group("B")], 2, :process => process)
      allow(instance).to receive(:dispatch_work).and_raise("coordinator bug")

      expect { instance.run }.to raise_error("coordinator bug")
      expect(process.signals.map(&:first)).to eq(%w[TERM TERM KILL KILL])
      expect(messages).to include("survived SIGKILL; giving up on it")
    end

    it "stops the workers when it is sent SIGTERM, then goes the way a serial run would" do
      handler = nil
      allow(RSpec::Core::Parallel).to receive(:trap_signals) do |signals, &block|
        expect(signals).to eq(%w[TERM HUP])
        handler = block
        { "TERM" => "DEFAULT" }
      end
      allow(RSpec::Core::Parallel).to receive(:restore_signals)
      groups = [passing_group("A"), RSpec.describe("Stuck") { it("sleeps") { sleep } }]
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
      signaller = Thread.new do
        sleep 0.01 until handler && process.signals.empty? && messages.include?(".")
        handler.call("TERM")
      end

      expect { coordinator(groups).run }.to raise_error(SignalException, "SIGTERM")
      signaller.join
      # The worker that ran A may not have been reaped yet, and gets one too.
      expect(process.signals.map(&:first).uniq).to eq(["TERM"])
      expect(messages).to include("Received SIGTERM; stopping the workers.", "2 examples, 0 failures")
      expect(RSpec::Core::Parallel).to have_received(:restore_signals).with("TERM" => "DEFAULT")
    end

    it "copes with a worker that is gone by the time it is signalled" do
      worker_behaviours[2] = :never_exits
      config.parallel_shutdown_timeout = 0.05
      allow(process).to receive(:kill).and_raise(Errno::ESRCH)

      exit_code = coordinator([passing_group("A"), passing_group("B")]).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(messages).to include("so it was sent SIGTERM", "sending SIGKILL", "giving up on it")
    end

    describe WorkerHandle do
      it "owns both ends of its two pipes and can close the coordinator's or all of them" do
        handle = WorkerHandle.new(1)
        expect([handle.jobs, handle.results, handle.child_jobs, handle.child_results]).to all(satisfy { |channel| !channel.closed? })

        handle.close_parent_ends
        expect([handle.jobs, handle.results]).to all(be_closed)
        expect([handle.child_jobs, handle.child_results]).to all(satisfy { |channel| !channel.closed? })

        handle.close_all
        expect([handle.jobs, handle.results, handle.child_jobs, handle.child_results]).to all(be_closed)
      end
    end

    it "treats a worker reaped by someone else as exited with an unknown status" do
      allow(process).to receive(:waitpid2).and_raise(Errno::ECHILD)
      config.parallel_shutdown_timeout = 0.05

      exit_code = coordinator([passing_group("A"), passing_group("B")]).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(messages).to include("exited unexpectedly (already reaped)")
    end

    it "bounds the wait for a worker whose results pipe closed while it was running" do
      config.parallel_shutdown_timeout = 0.05
      worker_behaviours[2] = :closes_results

      coordinator([passing_group("A"), passing_group("B")]).run

      expect(process.signals.map(&:first)).to eq(["TERM"])
      expect(messages).to include("stopped reporting for 0.05 seconds", "so it was sent SIGTERM")
    end
  end
end
