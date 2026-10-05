module RSpec::Core
  RSpec.describe Parallel do
    let(:config) { RSpec.configuration }
    let(:runner) { Runner.new(ConfigurationOptions.new([]), config, RSpec.world) }

    around { |example| without_env_vars("TEST_ENV_NUMBER", &example) }

    def groups(count)
      Array.new(count) { |i| RSpec.describe("G#{i}") { it("e") {} } }
    end

    describe ".coordinator_for" do
      it "returns nil when parallel execution was not requested" do
        expect(Parallel.coordinator_for(runner, groups(3))).to be_nil
      end

      it "returns a coordinator for the requested number of workers", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.parallel_workers = 2
        expect(Parallel.coordinator_for(runner, groups(3))).to be_a(Parallel::Coordinator)
      end

      it "uses one worker per processor for :auto", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.parallel_workers = :auto
        allow(Parallel).to receive(:default_worker_count).and_return(2)
        coordinator = instance_double(Parallel::Coordinator, :assign => nil)
        expect(Parallel::Coordinator).to receive(:new).with(runner, 2).and_return(coordinator)

        expect(Parallel.coordinator_for(runner, groups(5))).to eq(coordinator)
      end

      it "never starts more workers than there are groups with examples to run", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.parallel_workers = 8
        empty = RSpec.describe("empty")
        coordinator = instance_double(Parallel::Coordinator)
        expect(Parallel::Coordinator).to receive(:new).with(runner, 3).and_return(coordinator)
        expect(coordinator).to receive(:assign).with(anything, satisfy { |units| units.size == 3 })

        Parallel.coordinator_for(runner, groups(3) + [empty])
      end

      it "runs serially when fewer than two groups would be dispatched", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.parallel_workers = 4
        expect(Parallel.coordinator_for(runner, groups(1))).to be_nil
        expect(Parallel.coordinator_for(runner, [])).to be_nil
      end

      it "runs serially for one worker or a non-positive count forced from the command line", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.force(:parallel_workers => 1)
        expect(Parallel.coordinator_for(runner, groups(3))).to be_nil

        config.force(:parallel_workers => 0)
        expect(Parallel.coordinator_for(runner, groups(3))).to be_nil
      end

      it "runs serially, without a warning, under --dry-run" do
        config.parallel_workers = 2
        config.dry_run = true
        expect(RSpec).not_to receive(:warning)

        expect(Parallel.coordinator_for(runner, groups(3))).to be_nil
      end

      it "warns and runs serially where fork is not available" do
        config.parallel_workers = 2
        allow(RSpec::Support::RubyFeatures).to receive(:fork_supported?).and_return(false)
        expect(RSpec).to receive(:warning).with(a_string_including("needs `fork`", "Running serially"), :call_site => nil)

        expect(Parallel.coordinator_for(runner, groups(3))).to be_nil
      end

      it "warns and runs serially inside another tool's parallel worker", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.parallel_workers = 2
        with_env_vars("TEST_ENV_NUMBER" => "") do
          expect(RSpec).to receive(:warning).with(a_string_including("already a worker of a parallel test run"), :call_site => nil)

          expect(Parallel.coordinator_for(runner, groups(3))).to be_nil
        end
      end
    end

    describe ".suite_digest" do
      it "fingerprints the ids and locations of the examples to run, in load order, but not their descriptions" do
        RSpec.describe("A") do
          it("one") {}
          it("two") {}
        end
        RSpec.describe("B") { it("three") {} }
        first = Parallel.suite_digest(RSpec.world)

        expect(Parallel.suite_digest(RSpec.world)).to eq(first)
        RSpec.world.example_groups.last.examples.first.metadata[:full_description] = "B at #{Time.now}"
        expect(Parallel.suite_digest(RSpec.world)).to eq(first)
        RSpec.world.example_groups.last.examples.first.metadata[:location] = "./elsewhere_spec.rb:1"
        expect(Parallel.suite_digest(RSpec.world)).not_to eq(first)
      end

      it "changes with the examples defined" do
        RSpec.describe("A") { it("one") {} }
        first = Parallel.suite_digest(RSpec.world)
        RSpec.describe("B") { it("one more") {} }

        expect(Parallel.suite_digest(RSpec.world)).not_to eq(first)
      end

      it "is kept per spec file" do
        RSpec.describe("A") { it("one") {} }
        RSpec.describe("B") { it("two") {} }

        expect(Parallel.suite_digest(RSpec.world).keys).to eq([RSpec.world.example_groups.first.metadata[:file_path]])
      end

      it "leaves out the examples that are filtered out" do
        RSpec.describe("A") { it("one") {} }
        unfiltered = Parallel.suite_digest(RSpec.world)
        RSpec.describe("B") { it("skipped by the filter", :slow) {} }
        RSpec.world.filter_manager.exclude(:slow => true)

        expect(Parallel.suite_digest(RSpec.world)).to eq(unfiltered)
      end
    end

    describe ".default_worker_count" do
      it "is the number of processors" do
        require 'etc'
        expect(Parallel.default_worker_count).to eq(Etc.nprocessors)
      end
    end

    describe ".units_for" do
      it "skips groups without examples to run and keeps the given order without timing data" do
        all = groups(3)
        empty = RSpec.describe("empty")
        expect(Parallel.units_for([all[1], empty, all[0], all[2]], config)).to eq([all[1], all[0], all[2]])
      end

      context "with timings recorded in the example status persistence file", :isolated_directory do
        let(:all) { groups(4) }

        before do
          config.example_status_persistence_file_path = "statuses.txt"
          ExampleStatusPersister.persist([], "statuses.txt")
          File.write("statuses.txt", <<-STATUSES.gsub(/^ +\|/, ''))
            |example_id | status | run_time         |
            |---------- | ------ | ---------------- |
            |#{all[0].examples.first.id} | passed | 0.5 seconds      |
            |#{all[1].examples.first.id} | passed | 1 minute 2 seconds |
            |#{all[3].examples.first.id} | failed | 3 seconds        |
          STATUSES
        end

        it "puts groups with no recorded timing first, then the others longest first" do
          expect(Parallel.units_for(all, config)).to eq([all[2], all[1], all[3], all[0]])
        end

        it "keeps the given order for groups with equal timings" do
          File.write("statuses.txt", <<-STATUSES.gsub(/^ +\|/, ''))
            |example_id | status | run_time  |
            |---------- | ------ | --------- |
            |#{all[0].examples.first.id} | passed | 1 second  |
            |#{all[1].examples.first.id} | passed | 1 second  |
            |#{all[2].examples.first.id} | passed | 1 second  |
            |#{all[3].examples.first.id} | passed | 1 second  |
          STATUSES

          expect(Parallel.units_for(all.reverse, config)).to eq(all.reverse)
        end

        it "ignores an unreadable status file" do
          allow(ExampleStatusPersister).to receive(:load_from).and_raise(Errno::EACCES)
          expect(Parallel.units_for(all, config)).to eq(all)
        end
      end
    end

    describe ".parse_duration" do
      it "reads the durations written by the example status persister" do
        expect(Parallel.parse_duration("0.00012 seconds")).to eq(0.00012)
        expect(Parallel.parse_duration("1 second")).to eq(1.0)
        expect(Parallel.parse_duration("2 minutes 3.5 seconds")).to eq(123.5)
        expect(Parallel.parse_duration("")).to be_nil
        expect(Parallel.parse_duration(nil)).to be_nil
      end
    end

    describe ".marshal_safe" do
      it "returns values Marshal can dump and the block's value otherwise" do
        expect(Parallel.marshal_safe([1, "two"]) { :fallback }).to eq([1, "two"])
        expect(Parallel.marshal_safe(lambda {}) { |value| value.class }).to eq(Proc)
      end
    end

    describe ".monotonic_time" do
      it "never goes backwards" do
        first = Parallel.monotonic_time
        expect(Parallel.monotonic_time).to be >= first
      end
    end

    describe ".time_to_wire and .time_from_wire" do
      it "carry a local Time as integers and rebuild one equal to it, to the nanosecond" do
        time = ::Time.at(1_791_231_154, 100_773_123, :nsec)

        wire = Parallel.time_to_wire(time)
        rebuilt = Parallel.time_from_wire(Marshal.load(Marshal.dump(wire)))

        expect(wire).to eq([1_791_231_154, 100_773_123])
        expect(rebuilt).to eq(time)
        expect([rebuilt.nsec, rebuilt.utc_offset]).to eq([time.nsec, time.utc_offset])
      end

      it "leave nil, and values that are not on the wire, as they are" do
        time = ::Time.now

        expect(Parallel.time_to_wire(nil)).to be_nil
        expect(Parallel.time_from_wire(nil)).to be_nil
        expect(Parallel.time_from_wire(time)).to be(time)
      end
    end

    describe "signal handling", :skip => (Signal.list.key?("HUP") ? false : "needs SIGHUP") do
      around do |example|
        original = trap("HUP", "DEFAULT")
        begin
          example.run
        ensure
          trap("HUP", original || "DEFAULT")
        end
      end

      it "takes over a signal that would end the process and returns the handler it replaced" do
        received = []
        trap("HUP", "SYSTEM_DEFAULT")

        replaced = Parallel.trap_signals(%w[HUP NOSUCHSIGNAL]) { |signal| received << signal }
        trap("HUP", "DEFAULT").call

        expect(replaced).to eq("HUP" => "SYSTEM_DEFAULT")
        expect(received).to eq(["HUP"])
      end

      it "leaves an ignored signal ignored, as under nohup, and a handler the suite installed in place" do
        suite_handler = proc {}
        trap("HUP", "IGNORE")

        expect(Parallel.trap_signals(%w[HUP]) {}).to eq({})
        expect(trap("HUP", nil)).to eq("IGNORE")

        # How this Ruby reports a signal ignored with `nil` (MRI: `nil`, JRuby: "IGNORE").
        ignored_with_nil = trap("HUP", nil)
        expect(Parallel.trap_signals(%w[HUP]) {}).to eq({})
        expect(trap("HUP", suite_handler)).to eq(ignored_with_nil)

        expect(Parallel.trap_signals(%w[HUP]) {}).to eq({})
        expect(trap("HUP", "DEFAULT")).to equal(suite_handler)
      end

      it "reinstates the handlers it replaced" do
        Parallel.restore_signals("HUP" => "SYSTEM_DEFAULT")
        expect(trap("HUP", "IGNORE")).to eq("SYSTEM_DEFAULT")

        Parallel.restore_signals("HUP" => "DEFAULT")
        expect(trap("HUP", "IGNORE")).to eq("DEFAULT")
      end
    end
  end
end
