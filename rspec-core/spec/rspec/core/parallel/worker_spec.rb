require 'rspec/core/parallel/coordinator'

module RSpec::Core::Parallel
  RSpec.describe Worker do
    let(:jobs) { Channel.pipe }
    let(:results) { Channel.pipe }
    let(:exits) { [] }
    let(:stderr) { StringIO.new }
    let(:groups) do
      [
        RSpec.describe("First") { it("passes") {} },
        RSpec.describe("Second") do
          it("fails") { raise "nope" }
        end
      ]
    end

    let(:traps) { {} }

    def worker(number=2, options={})
      Worker.new(number, groups.map { |group| Unit.new(group) }, jobs.first, results.last,
                 { :stderr => stderr,
                   :exit => lambda { |status| exits << [:exit, status] },
                   :exit_now => lambda { |status| exits << [:exit_now, status] },
                   :trap => lambda { |signal, command=nil, &handler| traps[signal] = command || handler },
                   :watch_coordinator => false }.merge(options))
    end

    def frames
      results.first.drain.map(&:first)
    end

    after do
      jobs.each(&:close)
      results.each(&:close)
    end

    around { |example| without_env_vars("TEST_ENV_NUMBER", &example) }

    # A worker told to stop arms its exit watchdog, a thread that sleeps
    # through `Parallel.sleep_running` for up to the shutdown timeout. Workers
    # here are threads of this process, so it would outlive its example and
    # call a later example's stubs of `Parallel.sleep` too.
    around do |example|
      threads = Thread.list
      example.run
      (Thread.list - threads).each { |thread| thread.kill.join(1) }
    end

    describe "#test_env_number" do
      it "follows the parallel_tests convention: blank for the first worker, then the worker number" do
        expect(worker(1).test_env_number).to eq("")
        expect(worker(2).test_env_number).to eq("2")
        expect(worker(10).test_env_number).to eq("10")
      end
    end

    describe "#watch_coordinator" do
      it "exits at once, saying why, when its parent (the coordinating process) changes" do
        stub_const("RSpec::Core::Parallel::Worker::COORDINATOR_POLL", 0.01)
        parents = [100, 100, 100, 1]
        instance = Worker.new(2, groups, jobs.first, results.last,
                              :stderr => stderr,
                              :exit_now => lambda { |status| exits << [:exit_now, status] },
                              :ppid => lambda { parents.size > 1 ? parents.shift : parents.first })

        expect(instance.watch_coordinator.join(5)).to be_truthy
        expect(exits).to eq([[:exit_now, 1]])
        expect(stderr.string).to include("worker 2 (pid #{Process.pid}) is exiting because the coordinating process (pid 100) is gone")
      end

      it "still exits when it cannot say why (its stderr is gone too)" do
        stub_const("RSpec::Core::Parallel::Worker::COORDINATOR_POLL", 0.01)
        parents = [100, 1]
        instance = Worker.new(2, groups, jobs.first, results.last,
                              :stderr => StringIO.new.tap(&:close),
                              :exit_now => lambda { |status| exits << [:exit_now, status] },
                              :ppid => lambda { parents.size > 1 ? parents.shift : parents.first })

        expect(instance.watch_coordinator.join(5)).to be_truthy
        expect(exits).to eq([[:exit_now, 1]])
      end

      it "is started by #run unless disabled" do
        instance = Worker.new(2, [], jobs.first, results.last,
                              :stderr => stderr,
                              :exit => lambda { |status| exits << [:exit, status] },
                              :exit_now => lambda { |status| exits << [:exit_now, status] },
                              :trap => lambda { |*| },
                              :ppid => lambda { 100 })
        allow(instance).to receive(:watch_coordinator).and_return(nil)
        jobs.last.send([:stop])

        instance.run

        expect(instance).to have_received(:watch_coordinator)
        expect(exits).to eq([[:exit_now, 0]])
      end
    end

    describe "#prepare" do
      it "identifies the worker, disables autorun and installs the forwarding reporter" do
        worker(3).prepare

        expect(ENV['TEST_ENV_NUMBER']).to eq("3")
        expect(RSpec.configuration.parallel_worker_number).to eq(3)
        expect(RSpec.configuration.reporter).to be_a(WorkerReporter)
        expect(RSpec::Core::Runner).to be_autorun_disabled
      end

      it "handles interrupts like the runner does, but silently: stop between examples, force-quit the second time" do
        worker.prepare

        expect { traps.fetch('INT').call }.not_to output.to_stderr
        expect(RSpec.world.wants_to_quit).to be true
        expect(exits).to eq([])

        traps.fetch('INT').call
        expect(exits).to eq([[:exit_now, 1]])
      end

      it "reinstates the handlers the coordinating process replaced with its own, and only those" do
        worker(2, :signal_handlers => { "TERM" => "SYSTEM_DEFAULT", "HUP" => "DEFAULT" }).prepare

        expect(traps).to include("TERM" => "SYSTEM_DEFAULT", "HUP" => "DEFAULT")

        traps.clear
        worker.prepare

        expect(traps.keys).to eq(["INT"])
      end

      it "drops the formatters inherited from the coordinating process" do
        RSpec.configuration.formatter = "documentation"
        expect(RSpec.configuration.formatters).not_to be_empty

        worker.prepare

        expect(RSpec.configuration.formatters).to be_empty
      end
    end

    describe "#run" do
      it "runs the dispatched groups inside the suite hooks, reports each unit and exits normally when told to stop" do
        order = []
        RSpec.configuration.before(:suite) { order << :before_suite }
        RSpec.configuration.after(:suite) { order << :after_suite }
        groups.first.before(:context) { order << :group }

        jobs.last.send([:run, 0, groups[0].id])
        jobs.last.send([:run, 1, groups[1].id])
        jobs.last.send([:stop])

        worker.run

        expect(order).to eq([:before_suite, :group, :after_suite])
        expect(exits).to eq([[:exit, 0]])

        all = frames
        expect(all.select { |f| f.first == :unit_finished }).to eq([
          [:unit_finished, 0, true, false, false],
          [:unit_finished, 1, false, false, false]
        ])
        expect(all.last).to eq([:exiting, nil, nil])
        expect(all.map(&:first)).to include(:group_started, :example_started, :example_finished, :group_finished)
      end

      it "exits at once, without running suite hooks or at_exit handlers, when its first job is not a group to run" do
        hooks = []
        RSpec.configuration.before(:suite) { hooks << :before_suite }
        RSpec.configuration.after(:suite) { hooks << :after_suite }

        [[:stop], [:quit]].each do |job|
          exits.clear
          jobs.last.send(job)
          worker.run
          expect(exits).to eq([[:exit_now, 0]])
        end

        expect(hooks).to eq([])
        expect(frames).to eq([])
      end

      it "exits at once when the coordinator goes away before giving it anything" do
        jobs.last.close

        worker.run

        expect(exits).to eq([[:exit_now, 0]])
      end

      it "reports a quitting world and non-example failures with the unit" do
        RSpec.configuration.before(:suite) { raise "suite setup failed" }
        jobs.last.send([:run, 0, groups[0].id])
        jobs.last.send([:stop])

        worker.run

        unit = frames.find { |f| f.first == :unit_finished }
        expect(unit).to eq([:unit_finished, 0, false, true, true])
      end

      it "exits immediately without reporting when the results pipe is gone" do
        results.first.close
        jobs.last.send([:run, 0, groups[0].id])
        jobs.last.send([:stop])

        worker.run

        expect(exits).to eq([[:exit_now, 1]])
      end

      it "arms the watchdog before the after(:suite) hooks run" do
        armed_during_after_suite = nil
        instance = worker
        RSpec.configuration.after(:suite) { armed_during_after_suite = instance.instance_variable_get(:@watchdog_armed) }
        allow(instance).to receive(:arm_watchdog) { instance.instance_variable_set(:@watchdog_armed, true) }
        jobs.last.send([:run, 0, groups[0].id])
        jobs.last.send([:stop])

        instance.run

        expect(armed_during_after_suite).to be true
      end
    end

    describe "leaving its job loop" do
      # JRuby's `Example#run` rescues `SystemExit` (see example.rb), so there,
      # serially too, `exit` in an example fails it rather than ending the run.
      jruby_rescues_exit = RSpec::Support::Ruby.jruby? && "JRuby's Example#run rescues SystemExit"

      def leaving_frames
        frames.select { |frame| frame.first == :exiting }
      end

      it "arms the watchdog and says so before any after(:suite) hook, even one a before(:suite) hook added before calling exit" do
        instance = worker
        armed = []
        allow(instance).to receive(:arm_watchdog) { armed << :watchdog }
        config = RSpec.configuration
        config.before(:suite) do
          config.after(:suite) { armed << :late_after_suite }
          exit 4
        end
        jobs.last.send([:run, 0, groups[0].id])

        expect { instance.run }.to raise_error(SystemExit) { |error| expect(error.status).to eq(4) }

        expect(armed).to eq([:watchdog, :late_after_suite])
        expect(leaving_frames).to eq([[:exiting, "the code it ran called `exit` or `abort` (status 4)", 4]])
      end

      it "says which status the code it ran called `exit` with in an example, and runs nothing after it", :skip => jruby_rescues_exit do
        RSpec.configuration.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
        ran = []
        exiting = RSpec.describe("Exits") do
          it("exits") { exit 3 }
          it("never runs") { ran << :after }
        end
        instance = Worker.new(2, [Unit.new(exiting)], jobs.first, results.last,
                              :stderr => stderr, :exit => lambda { |_| }, :exit_now => lambda { |_| },
                              :trap => lambda { |*| }, :watch_coordinator => false)
        allow(instance).to receive(:arm_watchdog)
        jobs.last.send([:run, 0, exiting.id])
        jobs.last.send([:stop])

        expect { instance.run }.to raise_error(SystemExit) { |error| expect(error.status).to eq(3) }

        expect(ran).to eq([])
        expect(leaving_frames).to eq([[:exiting, "the code it ran called `exit` or `abort` (status 3)", 3]])
        expect(instance).to have_received(:arm_watchdog).once
      end

      it "says so again, with the status, when an after(:suite) hook calls exit" do
        instance = worker
        allow(instance).to receive(:arm_watchdog)
        RSpec.configuration.after(:suite) { exit 5 }
        jobs.last.send([:run, 0, groups[0].id])
        jobs.last.send([:stop])

        expect { instance.run }.to raise_error(SystemExit) { |error| expect(error.status).to eq(5) }

        expect(leaving_frames).to eq([[:exiting, nil, nil], [:exiting, "an `after(:suite)` hook called `exit` or `abort` (status 5)", 5]])
      end

      it "says so again when an after(:suite) hook calls exit with another status than the example did", :skip => jruby_rescues_exit do
        RSpec.configuration.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
        exiting = RSpec.describe("Exits") { it("exits") { exit 4 } }
        instance = Worker.new(2, [Unit.new(exiting)], jobs.first, results.last,
                              :stderr => stderr, :exit => lambda { |_| }, :exit_now => lambda { |_| },
                              :trap => lambda { |*| }, :watch_coordinator => false)
        allow(instance).to receive(:arm_watchdog)
        RSpec.configuration.after(:suite) { exit 5 }
        jobs.last.send([:run, 0, exiting.id])
        jobs.last.send([:stop])

        expect { instance.run }.to raise_error(SystemExit) { |error| expect(error.status).to eq(5) }

        expect(leaving_frames).to eq([
          [:exiting, "the code it ran called `exit` or `abort` (status 4)", 4],
          [:exiting, "an `after(:suite)` hook called `exit` or `abort` (status 5)", 5]
        ])
      end

      it "names a signal or another exception that ends the process" do
        expect(worker.exit_cause(SignalException.new("TERM"), "the code it ran")).to eq("it received SIGTERM")
        expect(worker.exit_cause(NoMemoryError.new("too much"), "the code it ran")).to eq(
          "the code it ran raised NoMemoryError (too much)"
        )
      end

      it "goes on without a watchdog when no thread can be started for it" do
        instance = worker
        allow(Thread).to receive(:new).and_raise(ThreadError, "can't create Thread: Resource temporarily unavailable")

        expect(instance.arm_watchdog).to be_nil
      end

      it "names why it is stopping in its thread dump" do
        RSpec.configuration.parallel_shutdown_timeout = 0.05
        instance = worker
        instance.instance_variable_set(:@exit_cause, "the code it ran called `exit` or `abort` (status 0)")

        instance.dump_threads(0.05)

        expect(stderr.string).to include(
          "did not exit within 0.05 seconds of stopping because the code it ran called `exit` or `abort` (status 0); aborting it."
        )
      end
    end

    describe "given a unit of some examples of a group" do
      let(:hooks) { [] }
      let(:group) do
        log = hooks
        RSpec.describe("Outer") do
          before(:context) { log << :outer }
          it("o1") { log << :o1 }
          context("A") do
            before(:context) { log << :a }
            it("a1") { log << :a1 }
            it("a2") { log << :a2 }
          end
          context("B") do
            before(:context) { log << :b }
            it("b1") { log << :b1 }
            it("b2") do |example|
              log << :b2
              raise "b2 fails" if example.metadata[:fail]
            end
          end
        end
      end

      def ids(*descriptions)
        group.descendants.flat_map(&:examples).select { |example| descriptions.include?(example.description) }.map(&:id)
      end

      def unit_worker
        Worker.new(2, [Unit.new(group)], jobs.first, results.last,
                   :stderr => stderr, :exit => lambda { |status| exits << [:exit, status] },
                   :exit_now => lambda { |status| exits << [:exit_now, status] },
                   :trap => lambda { |*| }, :watch_coordinator => false)
      end

      it "runs only those examples and the context hooks of the groups containing them, as `rspec path[ids]` would, and then the whole group as before" do
        RSpec.configuration.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
        jobs.last.send([:run, 0, group.id, ids("a2")])
        jobs.last.send([:run, 1, group.id, ids("o1", "b1")])
        jobs.last.send([:run, 2, group.id])
        jobs.last.send([:stop])

        unit_worker.run

        expect(hooks).to eq([:outer, :a, :a2, :outer, :o1, :b, :b1, :outer, :o1, :a, :a1, :a2, :b, :b1, :b2])
        started = frames.select { |frame| frame.first == :group_started }.map { |frame| frame[1] }
        expect(started.first(2)).to eq([group.id, group.children[0].id])
        expect(group.descendant_filtered_examples.size).to eq(5)
        expect(group.children.map { |child| child.descendant_filtered_examples.size }).to eq([2, 2])
      end

      it "restores what the group runs when a selected example fails" do
        RSpec.configuration.ordering_registry.register(:global, RSpec::Core::Ordering::Identity.new)
        group.children[1].examples.last.metadata[:fail] = true
        jobs.last.send([:run, 0, group.id, ids("b2")])
        jobs.last.send([:stop])

        unit_worker.run

        expect(frames.find { |frame| frame.first == :unit_finished }).to eq([:unit_finished, 0, false, false, false])
        expect(hooks).to eq([:outer, :b, :b2])
        expect(group.descendant_filtered_examples.map(&:description)).to eq(%w[o1 a1 a2 b1 b2])
        expect(RSpec.world.filtered_examples[group.children[1]].size).to eq(2)
      end
    end

    describe "forked before the spec files were loaded" do
      def loading_worker
        Worker.new(2, nil, jobs.first, results.last,
                   :stderr => stderr,
                   :exit => lambda { |status| exits << [:exit, status] },
                   :exit_now => lambda { |status| exits << [:exit_now, status] },
                   :trap => lambda { |*| },
                   :watch_coordinator => false)
      end

      def write_spec_file(dir, content)
        File.join(dir, "loaded_spec.rb").tap do |path|
          File.write(path, content)
          RSpec.configuration.files_or_directories_to_run = [path]
        end
      end

      # The worker blocks on the jobs channel once it has loaded the files.
      def run_in_thread(instance)
        Thread.new { instance.run }.tap { |thread| thread.report_on_exception = false }
      end

      it "loads them itself, reports what it loaded, then runs the groups it is given by id" do
        Dir.mktmpdir do |dir|
          write_spec_file(dir, "RSpec.describe('Loaded here') { it('passes') { } }")
          thread = run_in_thread(loading_worker)

          loaded = results.first.receive
          group = RSpec.world.example_groups.first
          expect(loaded).to eq([:loaded, RSpec::Core::Parallel.suite_digest(RSpec.world), nil])
          expect(group.description).to eq("Loaded here")

          jobs.last.send([:run, 0, group.id])
          jobs.last.send([:stop])
          expect(thread.join(10)).to be_truthy

          expect(exits).to eq([[:exit, 0]])
          expect(frames).to include([:unit_finished, 0, true, false, false])
        end
      end

      it "reports the first load error with what it loaded, and nothing else loading reported" do
        Dir.mktmpdir do |dir|
          write_spec_file(dir, "RSpec.deprecate('old_thing')\nraise 'boom at load'\n")
          thread = run_in_thread(loading_worker)

          kind, digest, failure = results.first.receive
          jobs.last.send([:stop])
          expect(thread.join(10)).to be_truthy

          expect([kind, digest]).to eq([:loaded, RSpec::Core::Parallel.suite_digest(RSpec.world)])
          expect(failure).to match(/\AAn error occurred while loading .*loaded_spec\.rb\. RuntimeError: boom at load\z/)
          expect(frames).to eq([])
          expect(RSpec.world.non_example_failure).to be_falsey
          expect(exits).to eq([[:exit_now, 0]])
        end
      end
    end

    describe "the watchdog" do
      it "dumps every thread's backtrace to stderr and aborts the process once the timeout passes" do
        RSpec.configuration.parallel_shutdown_timeout = 0.05
        instance = worker(4)
        blocked_line = __LINE__ + 1
        blocked = Thread.new { sleep }
        deadline = RSpec::Core::Parallel.monotonic_time + 10
        sleep 0.01 until blocked.status == "sleep" || RSpec::Core::Parallel.monotonic_time > deadline

        instance.arm_watchdog.join

        expect(exits).to eq([[:exit_now, 1]])
        expect(stderr.string).to include(
          "RSpec parallel worker 4 (pid #{Process.pid}) did not exit within 0.05 seconds",
          "(this watchdog)"
        )
        expect(stderr.string).to match(/worker_spec\.rb:#{blocked_line}:in /)
        expect(stderr.string.scan(/^  #<Thread:/).size).to be >= 3
      ensure
        blocked&.kill
      end

      it "still aborts the process when an example closed the stream it would dump the threads to" do
        RSpec.configuration.parallel_shutdown_timeout = 0.05
        stderr.close

        worker(4).arm_watchdog.join

        expect(exits).to eq([[:exit_now, 1]])
      end
    end
  end
end
