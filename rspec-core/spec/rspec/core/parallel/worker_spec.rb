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
      Worker.new(number, groups, jobs.first, results.last,
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
        expect(exits).to eq([[:exit, 0]])
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

        jobs.last.send([:run, 0])
        jobs.last.send([:run, 1])
        jobs.last.send([:stop])

        worker.run

        expect(order).to eq([:before_suite, :group, :after_suite])
        expect(exits).to eq([[:exit, 0]])

        all = frames
        expect(all.select { |f| f.first == :unit_finished }).to eq([
          [:unit_finished, 0, true, false, false],
          [:unit_finished, 1, false, false, false]
        ])
        expect(all.last).to eq([:exiting])
        expect(all.map(&:first)).to include(:group_started, :example_started, :example_finished, :group_finished)
      end

      it "treats a quit request received while idle like stop" do
        jobs.last.send([:quit])

        worker.run

        expect(exits).to eq([[:exit, 0]])
        expect(frames).to eq([[:exiting]])
      end

      it "stops when the coordinator goes away" do
        jobs.last.close

        worker.run

        expect(exits).to eq([[:exit, 0]])
      end

      it "reports a quitting world and non-example failures with the unit" do
        RSpec.configuration.before(:suite) { raise "suite setup failed" }
        jobs.last.send([:run, 0])
        jobs.last.send([:stop])

        worker.run

        unit = frames.find { |f| f.first == :unit_finished }
        expect(unit).to eq([:unit_finished, 0, false, true, true])
      end

      it "exits immediately without reporting when the results pipe is gone" do
        results.first.close
        jobs.last.send([:stop])

        worker.run

        expect(exits).to eq([[:exit_now, 1]])
      end

      it "arms the watchdog before the after(:suite) hooks run" do
        armed_during_after_suite = nil
        instance = worker
        RSpec.configuration.after(:suite) { armed_during_after_suite = instance.instance_variable_get(:@watchdog_armed) }
        allow(instance).to receive(:arm_watchdog) { instance.instance_variable_set(:@watchdog_armed, true) }
        jobs.last.send([:stop])

        instance.run

        expect(armed_during_after_suite).to be true
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
    end
  end
end
