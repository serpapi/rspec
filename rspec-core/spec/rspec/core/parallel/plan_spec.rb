require 'rspec/core/parallel/coordinator'

module RSpec::Core
  RSpec.describe Parallel::Plan do
    let(:config) { RSpec.configuration }
    let(:runner) { Runner.new(ConfigurationOptions.new([]), config, RSpec.world) }
    let(:plan) { Parallel::Plan.new(runner) }
    let(:started) { instance_double(Parallel::Coordinator, :start => nil, :started? => true, :assign => nil, :dismiss => nil) }

    around { |example| without_env_vars("TEST_ENV_NUMBER", &example) }

    def groups(count)
      Array.new(count) { |i| RSpec.describe("G#{i}") { it("e") {} } }
    end

    describe "by default, the spec files being loaded before the workers are forked" do
      it "forks nothing before loading, then leaves the decision to Parallel.coordinator_for" do
        config.parallel_workers = 2
        expect(Parallel::Coordinator).not_to receive(:new)
        plan.before_loading

        all = groups(3)
        expect(Parallel).to receive(:coordinator_for).with(runner, all).and_return(:coordinator)
        expect(plan.coordinator_for(all)).to eq(:coordinator)
      end
    end

    context "when every worker is to load the spec files itself", :skip => !RSpec::Support::RubyFeatures.fork_supported? do
      before { config.parallel_preload = false }

      def spec_files(count)
        allow(config).to receive(:files_to_run).and_return(Array.new(count) { |i| "./spec/f#{i}_spec.rb" })
      end

      it "forks the requested workers before loading, and gives them the groups" do
        config.parallel_workers = 3
        spec_files(5)
        expect(Parallel::Coordinator).to receive(:new).with(runner, 3).and_return(started)
        plan.before_loading
        expect(started).to have_received(:start)

        all = groups(1).tap { |list| list.first.describe("nested") { it("e") {} } } + groups(2)
        expect(plan.coordinator_for(all)).to be(started)
        expect(started).to have_received(:assign).with(all, all.map { |group| Parallel::Unit.new(group) })
      end

      it "forks no more workers than there are spec files to load" do
        config.parallel_workers = 8
        spec_files(3)
        expect(Parallel::Coordinator).to receive(:new).with(runner, 3).and_return(started)

        plan.before_loading
      end

      it "forks two workers for a single spec file, which may define several groups" do
        config.parallel_workers = 8
        spec_files(1)
        expect(Parallel::Coordinator).to receive(:new).with(runner, 2).and_return(started)

        plan.before_loading
      end

      it "dismisses the workers it forked when starting the others fails unexpectedly" do
        config.parallel_workers = 2
        failing = instance_double(Parallel::Coordinator, :dismiss => nil)
        allow(failing).to receive(:start).and_raise(RuntimeError, "unexpected")
        allow(Parallel::Coordinator).to receive(:new).and_return(failing)

        expect { plan.before_loading }.to raise_error(RuntimeError, "unexpected")
        plan.dismiss
        expect(failing).to have_received(:dismiss)
      end

      it "runs serially when no worker could be started" do
        config.parallel_workers = 2
        allow(Parallel::Coordinator).to receive(:new).and_return(started)
        allow(started).to receive(:started?).and_return(false)

        plan.before_loading

        expect(plan.coordinator_for(groups(3))).to be_nil
        expect(started).not_to have_received(:assign)
      end

      it "dismisses them, and runs serially, when fewer than two groups are to run" do
        config.parallel_workers = 3
        allow(Parallel::Coordinator).to receive(:new).and_return(started)
        plan.before_loading

        expect(plan.coordinator_for(groups(1))).to be_nil
        expect(started).to have_received(:dismiss)
      end

      it "runs serially, without forking or warning again, when a parallel run is not possible" do
        config.parallel_workers = 2
        allow(RSpec::Support::RubyFeatures).to receive(:fork_supported?).and_return(false)
        expect(RSpec).to receive(:warning).once.with(a_string_including("needs `fork`"), :call_site => nil)
        expect(Parallel::Coordinator).not_to receive(:new)

        plan.before_loading
        expect(plan.coordinator_for(groups(3))).to be_nil
      end

      it "forks nothing for one worker" do
        config.parallel_workers = 1
        expect(Parallel::Coordinator).not_to receive(:new)

        plan.before_loading
        expect(plan.coordinator_for(groups(3))).to be_nil
      end

      it "forks nothing before an --only-failures run without a status file, which aborts once the files are loaded" do
        config.parallel_workers = 2
        config.force(:only_failures => true)
        expect(Parallel::Coordinator).not_to receive(:new)

        plan.before_loading
      end

      it "warns, and runs serially, when the worker count is set while the spec files load" do
        plan.before_loading
        config.parallel_workers = 2
        expect(RSpec).to receive(:warning).with(a_string_including(
          "Ignoring `--parallel`: `config.parallel_workers` was set while the spec files were loading, too late to fork " \
          "the workers before them as `config.parallel_preload = false` asks."
        ).and(ending_with("Running serially")), :call_site => nil)
        expect(Parallel::Coordinator).not_to receive(:new)

        expect(plan.coordinator_for(groups(3))).to be_nil
      end

      it "warns, and runs serially, when the status file --only-failures needs is configured while the spec files load" do
        config.parallel_workers = 2
        config.force(:only_failures => true)
        plan.before_loading
        config.example_status_persistence_file_path = "examples.txt"
        expect(RSpec).to receive(:warning).with(a_string_including("`config.example_status_persistence_file_path` " \
                                                                   "(for `--only-failures`) was set while the spec files were loading"),
                                                :call_site => nil)

        expect(plan.coordinator_for(groups(3))).to be_nil
      end
    end

    it "warns, and runs serially, when told while the spec files load that each worker is to load them itself",
       :skip => !RSpec::Support::RubyFeatures.fork_supported? do
      config.parallel_workers = 2
      plan.before_loading
      config.parallel_preload = false
      expect(RSpec).to receive(:warning).with(a_string_including("`config.parallel_preload = false` was set while the spec " \
                                                                 "files were loading"), :call_site => nil)
      expect(Parallel::Coordinator).not_to receive(:new)

      expect(plan.coordinator_for(groups(3))).to be_nil
    end

    it "does not warn about the fork point of a run that is not parallel" do
      plan.before_loading
      config.parallel_preload = false
      expect(RSpec).not_to receive(:warning)

      expect(plan.coordinator_for(groups(3))).to be_nil
    end

    it "has nothing to dismiss when it forked nothing" do
      expect { plan.dismiss }.not_to raise_error
    end
  end
end
