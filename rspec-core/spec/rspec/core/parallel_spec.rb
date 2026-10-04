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
        expect(Parallel::Coordinator).to receive(:new).with(runner, anything, anything, 2).and_return(:coordinator)

        expect(Parallel.coordinator_for(runner, groups(5))).to eq(:coordinator)
      end

      it "never starts more workers than there are groups with examples to run", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
        config.parallel_workers = 8
        empty = RSpec.describe("empty")
        expect(Parallel::Coordinator).to receive(:new).with(runner, anything, anything, 3).and_return(:coordinator)

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
  end
end
