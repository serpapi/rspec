require 'rspec/core/parallel/coordinator'

# Loaded here: the fake workers below are threads of this process, and Ruby
# 3.0 warns ("loading in progress, circular require considered harmful") when
# a thread requires a file another thread is loading, which fails this suite.
require 'digest/sha2'

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

    attr_reader :signals, :warmups, :waits

    def threads
      @threads.values
    end

    def initialize(survives=[])
      @threads = {}
      @statuses = {}
      @signals = []
      @survives = survives
      @warmups = 0
      @waits = 0
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
      @waits += 1
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

  # Fake workers run their examples on threads of this process, side by side:
  # those examples stay off rspec-mocks' global space, which this spec's own
  # stubs live in and whose scopes are not meant to be shared between threads.
  module WithoutMocksLifecycle
    def setup_mocks_for_rspec; end

    def verify_mocks_for_rspec; end

    def teardown_mocks_for_rspec; end
  end

  RSpec.describe Coordinator, :parallel_split => 2 do
    let(:config) { RSpec.configuration }
    let(:world) { RSpec.world }
    let(:output) { StringIO.new }
    let(:runner) { RSpec::Core::Runner.new(RSpec::Core::ConfigurationOptions.new([]), config, world) }
    let(:process) { FakeProcess.new }
    let(:worker_stderr) { StringIO.new }
    let(:worker_exits) { {} }
    let(:worker_behaviours) { {} }

    around { |example| without_env_vars("TEST_ENV_NUMBER", &example) }

    # Fake workers are threads of this process, and one told to stop arms its
    # exit watchdog, a thread sleeping through `Parallel.sleep_running` for up
    # to the shutdown timeout: it would outlive its example and call a later
    # example's stubs of `Parallel.sleep` too.
    around do |example|
      threads = Thread.list
      example.run
      (Thread.list - threads).each { |thread| thread.kill.join(1) }
    end

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
        send_started(results, behaviour)
        case behaviour
        when :garbage        then stall_after { results.io.write("garbage\n") }
        when :partial_frame  then stall_after { results.io.write("999\nabc") }
        when :closes_results then stall_after { results.close }
        when :leaves_and_hangs then stall_after { leave_for_a_signal(jobs, results) }
        when :never_loads, :never_starts, :starts_once_asked_to_quit then stall(behaviour, handle, results)
        when :dies_loading   then Thread.current.kill
        else
          print_while_loading(handle, results) if behaviour == :prints_while_loading
          print_to(handle, results, "loaded quietly\n", "", false) if behaviour == :prints_unannounced_while_loading
          report_loaded(results, behaviour) if handle.loading
          send_extra_frames(results, behaviour, handle)
          run_fake_worker(handle.number, jobs, results, behaviour) { |status| exit_status = status }
        end
      ensure
        # Like the end of a process: its pipes close, and it is gone a moment later.
        [jobs, results].each(&:close)
        sleep 0.2 if behaviour == :slow_exit
      end

      @process.register(thread) { exit_status && FakeProcess::Status.new(exit_status, nil) }
    end

    # What a real worker sends first, which the fakes standing in for one send
    # too -- and then go on as they would, even if nothing reads it any more
    # (dismissed before their thread got going, say).
    def send_started(results, behaviour)
      fakes = [:garbage, :partial_frame, :closes_results, :leaves_and_hangs, :never_loads, :dies_loading]
      results.send([:started]) if fakes.include?(behaviour)
    rescue Errno::EPIPE, IOError
      nil
    end

    # One that reports back only once it has been asked to stop early stalls then.
    def stall(behaviour, handle, results)
      if behaviour == :starts_once_asked_to_quit
        sleep 0.01 until handle.quit_sent
        results.send([:started])
      end
      sleep
    end

    def stall_after
      yield
      sleep
    end

    # Takes its first job, then says it is leaving its job loop because of a
    # signal -- and never does.
    def leave_for_a_signal(jobs, results)
      jobs.receive
      results.send([:exiting, "it received SIGHUP", nil])
    end

    # Dies as it asks for its second job, as a process killed while it waits
    # for work would.
    def die_on_second_job(jobs)
      asked = 0
      jobs.define_singleton_method(:receive) do
        asked += 1
        Thread.current.kill if asked == 2
        super()
      end
    end

    # What a worker forked before loading reports once it has loaded the spec
    # files: by default, the examples this process has.
    def report_loaded(results, behaviour)
      digest = RSpec::Core::Parallel.suite_digest(world)
      digest = digest.merge("./spec/other_spec.rb" => "other examples") if behaviour == :loads_other_examples
      digest = digest.merge("./spec/a_spec.rb" => "a", "./spec/b_spec.rb" => "b") if behaviour == :loads_other_files
      failure = "An error occurred while loading ./spec/x_spec.rb. RuntimeError: boom" if behaviour == :fails_to_load
      results.send([:loaded, digest, failure])
    end

    def send_extra_frames(results, behaviour, handle)
      send_unknown_ids(results) if behaviour == :unknown_ids
      results.send([:expectations, 3]) if behaviour == :counts_expectations
      print_to(handle, results, "printed by worker #{handle.number}\n", "warned by worker #{handle.number}\n") if behaviour == :prints
      print_to(handle, results, "left behind by worker #{handle.number}\n", "", false) if behaviour == :prints_unannounced
    end

    # As a worker's own output goes to the files once it has moved its
    # standard output and error there; the frame is what it sends next.
    def print_to(handle, results, out, err, announce=true)
      File.open(handle.output.paths[:out], 'ab') { |file| file.write(out) }
      File.open(handle.output.paths[:err], 'ab') { |file| file.write(err) }
      results.send([:output, handle.output.paths.transform_values { |path| File.size(path) }]) if announce
    end

    def print_while_loading(handle, results)
      print_to(handle, results, "noise while loading\n", "")
    end

    def send_unknown_ids(results)
      results.send([:example_started, "./nope_spec.rb[1:1]"])
      results.send([:example_started, "./nope_spec.rb[1:1]"])
      results.send([:group_started, "./nope_spec.rb[1]"])
      results.send([:weird])
    end

    def run_fake_worker(number, jobs, results, behaviour, &record_exit)
      die_on_second_job(jobs) if behaviour == :dies_between_groups
      worker_config = RSpec::Core::Configuration.new
      worker_config.parallel_shutdown_timeout = 60
      Thread.current[:fake_worker_world] = RSpec::Core::World.new(worker_config)
      Worker.new(number, units_of(@units), jobs, results,
                 :configuration => worker_config,
                 :world => Thread.current[:fake_worker_world],
                 :stderr => worker_stderr,
                 :exit => lambda { |status|
                   record_exit.call(status)
                   sleep if behaviour == :never_exits
                   exit_once_sent_term(results) if behaviour == :exits_once_sent_term
                 },
                 :exit_now => record_exit,
                 :trap => lambda { |*| },
                 :watch_coordinator => false).run
    end

    # Its teardown hangs until it is sent SIGTERM, which an `after(:suite)`
    # hook takes by calling `exit 0` -- and hangs again.
    def exit_once_sent_term(results)
      sleep 0.01 until @process.signals.any? { |name, _pid| name == 'TERM' }
      begin
        results.send([:exiting, "an `after(:suite)` hook called `exit` or `abort` (status 0)", 0])
      rescue Errno::EPIPE, IOError
        nil # as Worker#send_exiting does once nothing reads it
      end
      sleep
    end

    # A fake worker killed with a signal is gone once its thread has died,
    # which a loaded machine can delay: the grace periods are long unless a
    # spec is about them, so that a slow death is not escalated.
    def new_coordinator(units, worker_count, options)
      units.each { |group| group.include(WithoutMocksLifecycle) }
      @units = units
      @process = options.fetch(:process, process)
      Coordinator.new(runner, worker_count, { :process => @process, :spawn => method(:spawn), :tick => 0.01,
                                              :term_grace => 5, :kill_grace => 5, :watchdog_grace => 0.05 }.merge(options))
    end

    def coordinator(groups, worker_count=2, options={})
      new_coordinator(groups, worker_count, options).tap { |instance| instance.assign(groups, units_of(groups)) }
    end

    def units_of(groups)
      groups.map { |group| Unit.new(group) }
    end

    # Started the way `--no-parallel-preload` starts it: the workers are forked
    # before the groups to run are known, and load the spec files themselves.
    def started_coordinator(units, worker_count=2, options={})
      new_coordinator(units, worker_count, options).tap(&:start)
    end

    let(:short_graces) { { :term_grace => 0.1, :kill_grace => 0.1 } }

    def passing_group(name)
      RSpec.describe(name) { it("passes") {} }
    end

    def failing_group(name)
      RSpec.describe(name) { it("fails") { raise "boom" } }
    end

    def messages
      output.string
    end

    it "checks for exited workers a bounded number of times while results stream in, not on every wake-up" do
      groups = %w[A B].map { |name| RSpec.describe(name) { 400.times { |i| it("passes #{i}") {} } } }
      started = RSpec::Core::Parallel.monotonic_time

      expect(coordinator(groups).run).to eq(0)

      elapsed = RSpec::Core::Parallel.monotonic_time - started
      expect(messages).to include("800 examples, 0 failures")
      expect(process.waits).to be <= 2 * ((elapsed / WorkerLifecycle::REAP_INTERVAL).ceil + 6)
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

    it "reaps workers as soon as they are gone rather than a tick after their results pipe closed" do
      worker_behaviours[1] = worker_behaviours[2] = :slow_exit
      started = RSpec::Core::Parallel.monotonic_time

      exit_code = coordinator([passing_group("A"), passing_group("B")], 2, :tick => 5).run

      expect(exit_code).to eq(0)
      expect(RSpec::Core::Parallel.monotonic_time - started).to be < 2
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

    it "bounds the wait for a worker that left its job loop for a signal and does not exit, and runs its group elsewhere" do
      config.parallel_shutdown_timeout = 0.05
      worker_behaviours[1] = :leaves_and_hangs
      groups = [passing_group("A"), passing_group("B")]

      exit_code = coordinator(groups, 2, short_graces).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(process.signals.map(&:first)).to include("TERM")
      expect(messages).to include(
        "did not exit within 0.05 seconds of leaving its job loop (its `after(:suite)` hooks or `at_exit` handlers may " \
        "be stuck; raise `config.parallel_shutdown_timeout` if they legitimately need longer) before it started A",
        ": it received SIGHUP; 1 example of A",
        "2 examples, 0 failures, 1 error occurred outside of examples"
      )
    end

    describe "a worker that exits while running a group" do
      it "fails the example it was running with the reason, never runs it again, and runs the rest of the group in another worker" do
        config.error_exit_code = 5
        runs = Hash.new(0)
        crashing = RSpec.describe("Crashing") do
          it("finishes") { runs[:finishes] += 1 }
          it("dies") do
            runs[:dies] += 1
            Thread.current.kill
          end
          it("runs elsewhere") { runs[:elsewhere] += 1 }
        end
        groups = [crashing, passing_group("B")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        dies = crashing.examples[1]
        expect(crashing.examples.map { |e| e.execution_result.status }).to eq([:passed, :failed, :passed])
        expect(runs).to eq(:finishes => 1, :dies => 1, :elsewhere => 1)
        expect(dies.exception).to be_a(WorkerError)
        expect(dies.exception.message).to match(
          /\AParallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 1\) while running this example; 1 example of Crashing \(.+\) that it had not started yet will run in another worker\.\z/
        )
        expect(dies.exception.backtrace).to eq([dies.location])
        expect(dies.execution_result.run_time).to be >= 0
        expect(messages).to include("Crashing dies", "4 examples, 1 failure", "rspec #{dies.location_rerun_argument}")
      end

      it "fails the unstarted examples of the group it was in when it was not running one, as a failing context hook does" do
        config.error_exit_code = 5
        crashing = RSpec.describe("Crashing") do
          before(:context) { Thread.current.kill }
          it("cannot run") {}
          it("cannot run either") {}
        end
        groups = [crashing, passing_group("B")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(crashing.examples.map { |e| e.execution_result.status }).to eq([:failed, :failed])
        expect(crashing.examples.first.exception.message).to match(
          /\AParallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 1\) while running Crashing \(.+\), outside of any example, so this example could not run\.\z/
        )
        expect(messages).to include("3 examples, 2 failures", "rspec #{crashing.examples.first.location_rerun_argument}")
        expect(messages).not_to include("outside of examples")
      end
    end

    it "reports a worker that exits between groups, while it waits for a busy one, as an error of the run" do
      worker_behaviours[1] = :dies_between_groups
      reported = output
      busy = RSpec.describe("Busy") do
        it("passes once worker 1 is reported lost") do
          give_up_at = RSpec::Core::Parallel.monotonic_time + 10
          sleep 0.01 until reported.string.include?("between example groups") || RSpec::Core::Parallel.monotonic_time > give_up_at
        end
      end
      groups = [passing_group("A"), busy]
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

      exit_code = coordinator(groups).run

      expect(exit_code).to eq(config.failure_exit_code)
      expect(groups.map { |group| group.examples.first.execution_result.status }).to eq([:passed, :passed])
      expect(messages).to match(/Parallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 1\) between example groups\./)
      expect(messages).to include("2 examples, 0 failures, 1 error occurred outside of examples")
    end

    # A single worker, so that no well-behaved one can miss a deadline this
    # short on a loaded machine.
    describe "a worker that does not exit after finishing" do
      before do
        config.parallel_shutdown_timeout = 0.05
        worker_behaviours[1] = :never_exits
      end

      it "is sent SIGTERM after the shutdown timeout and the run fails with the error exit code" do
        config.error_exit_code = 3

        exit_code = coordinator([passing_group("A"), passing_group("B")], 1).run

        expect(exit_code).to eq(3)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(messages).to include("did not exit within 0.05 seconds of finishing its last example group", "so it was sent SIGTERM")
        expect(messages).to include("2 examples, 0 failures, 1 error occurred outside of examples")
      end

      it "is sent SIGKILL when it survives SIGTERM" do
        process = FakeProcess.new(["TERM"])

        exit_code = coordinator([passing_group("A"), passing_group("B")], 1, short_graces.merge(:process => process)).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(%w[TERM KILL])
        expect(messages).to include("still alive 0.1 seconds after SIGTERM; sending SIGKILL")
      end

      it "is given up on, so the run still ends, when it survives SIGKILL" do
        process = FakeProcess.new(%w[TERM KILL])
        started = RSpec::Core::Parallel.monotonic_time

        exit_code = coordinator([passing_group("A"), passing_group("B")], 1, short_graces.merge(:process => process)).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(%w[TERM KILL])
        expect(messages).to include("still alive 0.1 seconds after SIGKILL; giving up on it")
        expect(RSpec::Core::Parallel.monotonic_time - started).to be < 5
      end

      it "does not let an `exit` it calls once sent SIGTERM decide the run's exit status" do
        config.error_exit_code = 3
        worker_behaviours[1] = :exits_once_sent_term
        process = FakeProcess.new(["TERM"])

        # Rescued: an `exit` deciding the status would end this suite here.
        exit_code =
          begin
            coordinator([passing_group("A"), passing_group("B")], 1, short_graces.merge(:process => process)).run
          rescue SystemExit => e
            "exit(#{e.status})"
          end

        expect(exit_code).to eq(3)
        expect(process.signals.map(&:first)).to eq(%w[TERM KILL])
        expect(messages).not_to include("ending the run")
      end
    end

    # Long enough a timeout for the well-behaved worker to stop within it even
    # on a loaded machine.
    describe "a worker that is asked to stop early but does not" do
      before { config.parallel_shutdown_timeout = 0.5 }

      it "is terminated after the shutdown timeout even when it reports back only after it was asked to stop" do
        config.fail_fast = true
        worker_behaviours[2] = :starts_once_asked_to_quit
        groups = [failing_group("Fails"), passing_group("Held by the late worker")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        run = Thread.new { coordinator(groups).run }

        expect(run.join(10)).to be_truthy
        expect(run.value).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(messages).to include("did not stop within 0.5 seconds of being asked to stop early")
      end

      it "is terminated after the shutdown timeout so a cancelled run still ends" do
        config.fail_fast = true
        worker_behaviours[2] = :partial_frame
        groups = [failing_group("Fails"), passing_group("Held by the stalled worker")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(messages).to include("did not stop within 0.5 seconds of being asked to stop early")
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
        config.fail_fast = true
        groups = failure_once_running("Stuck") { sleep }
        stuck = groups.last

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(process.signals.map(&:first)).to eq(["TERM"])
        expect(stuck.examples.first.exception.message).to match(
          /\AParallel worker 2 \(pid \d+\) did not stop within 0\.5 seconds of being asked to stop early \(fail-fast or interrupt\) while running this example, so it was sent SIGTERM\.\z/
        )
        expect(messages).to include("2 examples, 2 failures")
      end

      it "ignores what it sends once it was given up on, so that its example is reported once" do
        config.fail_fast = true
        groups = failure_once_running("Late") { sleep 1.5 }

        exit_code = coordinator(groups, 2, :process => FakeProcess.new(["TERM"]), :term_grace => 5).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(groups.last.examples.first.exception).to be_a(WorkerError)
        expect(messages).to include("2 examples, 2 failures")
        expect(messages).not_to include("sending SIGKILL")
      end
    end

    describe "with workers forked before the spec files are loaded" do
      def run_started(units, worker_count=2, options={})
        started_coordinator(units, worker_count, options).tap { |instance| instance.assign(units, units_of(units)) }.run
      end

      it "gives work to the workers that report having loaded the examples this process has" do
        groups = %w[A B C].map { |name| passing_group(name) }

        exit_code = run_started(groups)

        expect(exit_code).to eq(0)
        expect(messages).to include("3 examples, 0 failures")
        expect(process.signals).to be_empty
      end

      it "does not use a worker that loaded other examples, and runs its share elsewhere" do
        config.error_exit_code = 5
        worker_behaviours[2] = :loads_other_examples
        groups = %w[A B C].map { |name| passing_group(name) }

        exit_code = run_started(groups)

        expect(exit_code).to eq(5)
        expect(messages).to match(%r{Parallel worker 2 \(pid \d+\) loaded other examples than this process from \./spec/other_spec\.rb \(.*--parallel-preload.*, so it was not used\.})
        expect(messages).to include("3 examples, 0 failures, 1 error occurred outside of examples")
        expect(process.signals.map(&:first)).to eq(["KILL"])
      end

      it "names the first spec file a worker loaded differently and counts the others" do
        worker_behaviours[2] = :loads_other_files
        groups = %w[A B].map { |name| passing_group(name) }

        run_started(groups)

        expect(messages).to match(%r{loaded other examples than this process from \./spec/a_spec\.rb and 1 more spec file \(})
      end

      it "does not use a worker whose loading failed when this process's did not" do
        worker_behaviours[2] = :fails_to_load
        groups = %w[A B].map { |name| passing_group(name) }

        run_started(groups)

        expect(messages).to include("Parallel worker 2 (pid #{process.signals.first.last}) could not load the spec files: " \
                                    "An error occurred while loading ./spec/x_spec.rb. RuntimeError: boom, so it was not used.")
        expect(messages).to include("2 examples, 0 failures, 1 error occurred outside of examples")
      end

      it "uses a worker whose loading failed the way this process's did, like a serial run goes on" do
        worker_behaviours[2] = :fails_to_load
        groups = %w[A B].map { |name| passing_group(name) }
        world.non_example_failure = true

        run_started(groups)

        expect(messages).not_to include("could not load")
        expect(process.signals).to be_empty
        expect(groups.flat_map(&:examples).map { |e| e.execution_result.status }).to all(eq(:passed))
      end

      it "terminates a worker still loading well after this process finished, and runs its share elsewhere" do
        config.parallel_shutdown_timeout = 0.1
        worker_behaviours[2] = :never_loads
        groups = %w[A B C].map { |name| RSpec.describe(name) { it("takes a while") { sleep 0.2 } } }

        run_started(groups)

        expect(messages).to match(/Parallel worker 2 \(pid \d+\) took longer than [\d.]+ seconds? while loading the spec files, so it was sent SIGTERM\./)
        expect(messages).to include("3 examples, 0 failures, 1 error occurred outside of examples")
        expect(process.signals.map(&:first)).to eq(["TERM"])
      end

      it "dismisses a worker still loading once nothing is left for it" do
        worker_behaviours[2] = :never_loads
        groups = %w[A B].map { |name| passing_group(name) }

        exit_code = run_started(groups)

        expect(exit_code).to eq(0)
        expect(process.signals.map(&:first)).to eq(["KILL"])
        expect(messages).not_to include("Parallel worker")
      end

      it "reports a worker that exits while loading" do
        worker_behaviours[2] = :dies_loading
        groups = %w[A B].map { |name| passing_group(name) }

        run_started(groups)

        expect(messages).to match(/Parallel worker 2 \(pid \d+\) exited unexpectedly \(exit status 1\) while loading the spec files\./)
        expect(messages).to include("2 examples, 0 failures, 1 error occurred outside of examples")
      end

      it "dismisses the workers there are more of than groups to run" do
        groups = %w[A B].map { |name| passing_group(name) }

        exit_code = run_started(groups, 3)

        expect(exit_code).to eq(0)
        expect(process.signals.map(&:first)).to eq(["KILL"])
        expect(messages).to include("2 examples, 0 failures")
      end

      it "dismisses every worker when the run does not need them, giving up on one that survives SIGKILL" do
        worker_behaviours[1] = worker_behaviours[2] = :never_loads
        survivor = FakeProcess.new(["KILL"])
        instance = started_coordinator([passing_group("A")], 2, :process => survivor, :kill_grace => 0.1)

        instance.dismiss

        expect(survivor.signals.map(&:first)).to eq(%w[KILL KILL])
        expect(messages.scan("survived SIGKILL; giving up on it.").size).to eq(2)
        instance.dismiss
        expect(survivor.signals.size).to eq(2)
      ensure
        survivor.threads.each(&:kill)
      end

      it "goes on with the workers it could fork when a later fork fails, and dismisses only those" do
        worker_behaviours[1] = :never_loads
        fork_fails = lambda do |handle, handles|
          raise Errno::EAGAIN, "fork(2)" if handle.number == 2

          spawn(handle, handles)
        end
        instance = new_coordinator([passing_group("A")], 3, :spawn => fork_fails)
        expect(RSpec).to receive(:warning).with("Started 1 of 3 parallel workers: the next could not be started " \
                                                "(Errno::EAGAIN: Resource temporarily unavailable - fork(2)). Going on without it",
                                                :call_site => nil)

        instance.start
        expect(instance).to be_started
        started = RSpec::Core::Parallel.monotonic_time
        instance.dismiss

        expect(process.signals.map(&:first)).to eq(["KILL"])
        expect(process.threads).to all(satisfy { |thread| !thread.alive? })
        expect(messages).not_to include("survived SIGKILL")
        expect(RSpec::Core::Parallel.monotonic_time - started).to be < 4
      end
    end

    describe "pauses of the whole run (Ctrl-Z, a sleeping machine)" do
      let(:clock) { [10.0] }
      let(:instance) { coordinator([passing_group("A")]) }
      let(:workers) { Array.new(2) { |i| RSpec::Core::Parallel::WorkerHandle.new(i + 1) } }

      before do
        allow(RSpec::Core::Parallel).to receive(:monotonic_time) { clock.first }
        instance.instance_variable_get(:@workers).concat(workers)
        instance.instance_variable_set(:@last_turn_at, clock.first)
      end

      after { workers.each(&:close_all) }

      def turn(seconds)
        clock[0] += seconds
        instance.send(:account_for_pauses)
      end

      it "move a deadline back by the time the loop was paused beyond its own waits" do
        workers[0].deadline = 50.0
        turn(0.2)
        turn(20.0)

        expect(workers[0].deadline).to be_within(0.001).of(50.0 + 20.0 - 0.01 - RSpec::Core::Parallel::PAUSE_TOLERANCE)
      end

      it "leave a deadline set after the pause, in the same turn, as it is" do
        workers[0].deadline = 50.0
        clock[0] += 20.0
        workers[1].deadline = 60.0
        turn(0.1)

        expect(workers[0].deadline).to be > 65.0
        expect(workers[1].deadline).to eq(60.0)
      end

      it "leave deadlines alone in turns of normal length" do
        workers[0].deadline = 50.0
        5.times { turn(0.25) }

        expect(workers[0].deadline).to eq(50.0)
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

    it "adds the expectations a worker counted to its reporter's count, when that keeps one" do
      worker_behaviours[1] = :counts_expectations
      counted = 0
      config.reporter.define_singleton_method(:increment_expectation_count) { counted += 1 }

      exit_code = coordinator([passing_group("A"), passing_group("B")]).run
      # Before this spec's own expectations, which an rspec-expectations that counts them adds to.
      counted_by_the_run = counted

      expect(exit_code).to eq(0)
      expect(counted_by_the_run).to eq(3)
      expect(messages).not_to include("unexpected")
    end

    it "ignores the expectations a worker counted when its reporter keeps no count" do
      worker_behaviours[1] = :counts_expectations
      allow(config.reporter).to receive(:respond_to?).and_call_original
      allow(config.reporter).to receive(:respond_to?).with(:increment_expectation_count).and_return(false)

      exit_code = coordinator([passing_group("A"), passing_group("B")]).run

      expect(exit_code).to eq(0)
      expect(messages).not_to include("unexpected")
      expect(messages).to include("2 examples, 0 failures")
    end

    it "shows what workers printed, where they reported it, and what they left unannounced once they are gone" do
      worker_behaviours[1] = :prints
      worker_behaviours[2] = :prints_unannounced
      exit_code = nil

      stdout, stderr = standard_streams { exit_code = coordinator([passing_group("A"), passing_group("B")]).run }

      expect(exit_code).to eq(0)
      expect(stdout).to include("printed by worker 1\n", "left behind by worker 2\n")
      expect(stderr).to eq("warned by worker 1\n")
    end

    it "does not show what a worker that loaded the spec files itself printed while it did" do
      worker_behaviours[1] = :prints_while_loading
      instance = started_coordinator([passing_group("A"), passing_group("B")])
      instance.assign(@units, units_of(@units))

      exit_code = nil
      stdout, _stderr = standard_streams { exit_code = instance.run }

      expect(exit_code).to eq(0)
      expect(stdout).to eq("")
    end

    it "does not show what a worker dismissed before the run used it left in its files" do
      worker_behaviours[3] = :prints_unannounced_while_loading
      instance = started_coordinator([passing_group("A"), passing_group("B")], 3)
      instance.assign(@units, units_of(@units))

      exit_code = nil
      stdout, _stderr = standard_streams { exit_code = instance.run }

      expect(exit_code).to eq(0)
      expect(stdout).to eq("")
    end

    it "goes on when it cannot show what a worker printed" do
      worker_behaviours[1] = :prints
      exit_code = nil

      standard_streams(StringIO.new.tap(&:close_write)) { exit_code = coordinator([passing_group("A"), passing_group("B")]).run }

      expect(exit_code).to eq(0)
      expect(messages).to include("2 examples, 0 failures")
    end

    # What the coordinator writes to them; the formatters write to `output`.
    def standard_streams(stdout=StringIO.new)
      streams = [$stdout, $stderr]
      $stdout = stdout
      $stderr = StringIO.new
      yield
      [$stdout.string, $stderr.string]
    ensure
      $stdout, $stderr = streams
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

    it "runs every group in the workers it could start when the system refuses more" do
      failing_spawn = lambda do |handle, handles|
        raise Errno::EMFILE if handle.number > 1

        spawn(handle, handles)
      end
      expect(RSpec).to receive(:warning).with(a_string_starting_with("Started 1 of 3 parallel workers: the next could not " \
                                                                     "be started (Errno::EMFILE: Too many open files)"),
                                              :call_site => nil)

      exit_code = coordinator(%w[A B C].map { |name| passing_group(name) }, 3, :spawn => failing_spawn).run

      expect(exit_code).to eq(0)
      expect(messages).to include("3 examples, 0 failures")
      expect(process.signals).to be_empty
    end

    it "runs the groups in this process, like a serial run, when it cannot start any worker" do
      ran_in = []
      groups = %w[A B].map { |name| RSpec.describe(name) { it("records its process") { ran_in << Process.pid } } }
      config.before(:suite) { ran_in << :before_suite }
      expect(RSpec).to receive(:warning).with(a_string_including("Started 0 of 2 parallel workers").and(ending_with("Running serially")),
                                              :call_site => nil)

      exit_code = coordinator(groups, 2, :spawn => lambda { |*| raise Errno::EAGAIN, "fork" }).run

      expect(exit_code).to eq(0)
      expect(ran_in).to eq([:before_suite, Process.pid, Process.pid])
      expect(messages).to include("2 examples, 0 failures")
      expect(Signal.trap("TERM", "DEFAULT")).not_to be_a(Proc)
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

    describe "a worker that never reports back after it was forked" do
      it "is terminated once it misses its start deadline, and the group it was given runs in another worker" do
        config.parallel_shutdown_timeout = 0.05
        worker_behaviours[2] = :never_starts

        exit_code = coordinator([passing_group("A"), passing_group("B")]).run

        expect(exit_code).to eq(0)
        expect(messages).to include("2 examples, 0 failures")
        expect(messages.scan("never reported back after it was forked").size).to eq(1)
        expect(messages).to include("the group it was given runs in another worker")
        expect(process.signals.map(&:first)).to eq(["TERM"])
      end

      it "says the group it was given did not run when the run stops early" do
        config.parallel_shutdown_timeout = 0.05
        config.fail_fast = true
        worker_behaviours[2] = :never_starts
        groups = [failing_group("Fails"), passing_group("Held by the stuck worker")]
        config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)

        exit_code = coordinator(groups).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(messages).to include("never reported back after it was forked", "the group it was given did not run")
        expect(messages).to include("1 example, 1 failure")
      end

      it "is terminated once it misses its start deadline, and what no worker ran is reported" do
        config.parallel_shutdown_timeout = 0.05
        worker_behaviours[1] = worker_behaviours[2] = :never_starts

        exit_code = coordinator([passing_group("A"), passing_group("B")]).run

        expect(exit_code).to eq(config.failure_exit_code)
        expect(messages.scan("never reported back after it was forked").size).to eq(2)
        expect(messages).to include("did not run because no worker remained to run them")
        expect(process.signals.map(&:first)).to eq(%w[TERM TERM])
      end
    end

    it "requeues a group when its worker cannot be reached and lets another worker run it" do
      config.parallel_shutdown_timeout = 5
      # Its pipe closes as the first job is written to it: the write fails,
      # and the worker sees the end of it, as when the coordinator is gone.
      spawn_with_closed_jobs = lambda do |handle, handles|
        pid = spawn(handle, handles)
        if handle.number == 2
          handle.jobs.define_singleton_method(:send) do |message|
            io.close_write
            super(message)
          end
        end
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
      instance = coordinator([passing_group("A"), passing_group("B")], 2, short_graces.merge(:process => process))
      allow(instance).to receive(:dispatch_work).and_raise("coordinator bug")

      expect { instance.run }.to raise_error("coordinator bug")
      expect(process.signals.map(&:first)).to eq(%w[TERM TERM KILL KILL])
      expect(messages).to include("survived SIGKILL; giving up on it")
    end

    it "stops the workers when it is sent SIGTERM, saying what they were running, then goes the way a serial run would" do
      handler = nil
      allow(RSpec::Core::Parallel).to receive(:trap_signals) do |signals, &block|
        expect(signals).to eq(%w[TERM HUP])
        handler = block
        { "TERM" => "DEFAULT" }
      end
      allow(RSpec::Core::Parallel).to receive(:restore_signals)
      stuck_started = Queue.new
      stuck = RSpec.describe("Stuck") do
        it("sleeps") do
          stuck_started << true
          sleep
        end
      end
      groups = [passing_group("A"), stuck]
      config.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
      signaller = Thread.new do
        sleep 0.01 until handler && process.signals.empty? && messages.include?(".") && !stuck_started.empty?
        handler.call("TERM")
      end

      expect { coordinator(groups).run }.to raise_error(SignalException, "SIGTERM")
      signaller.join
      # The worker that ran A may not have been reaped yet, and gets one too.
      expect(process.signals.map(&:first).uniq).to eq(["TERM"])
      expect(messages).to include("Received SIGTERM; stopping the workers.")
      # Not defined in a loaded spec file, so it is named by its id rather than its line.
      expect(messages).to match(
        /^Parallel worker 2 \(pid \d+\) was running #{Regexp.escape(stuck.examples.first.id)} \(Stuck sleeps\) for [\d.]+ seconds?\.$/
      )
      expect(messages).to match(/^2 examples, 0 failures(, .+)?$/)
      expect(messages).not_to include("outside of examples")
      expect(stuck.examples.first.execution_result.status).to be_nil
      expect(RSpec::Core::Parallel).to have_received(:restore_signals).with("TERM" => "DEFAULT")
    end

    it "copes with a worker that is gone by the time it is signalled" do
      worker_behaviours[2] = :never_exits
      config.parallel_shutdown_timeout = 0.05
      allow(process).to receive(:kill).and_raise(Errno::ESRCH)

      exit_code = coordinator([passing_group("A"), passing_group("B")], 2, short_graces).run

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
      worker_behaviours[1] = :closes_results

      coordinator([passing_group("A"), passing_group("B")], 1).run

      expect(process.signals.map(&:first)).to eq(["TERM"])
      expect(messages).to include("stopped reporting for 0.05 seconds", "so it was sent SIGTERM")
    end
  end
end
