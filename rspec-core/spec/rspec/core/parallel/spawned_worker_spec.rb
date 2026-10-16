require 'tempfile'
require 'rspec/core/parallel/coordinator'

module RSpec::Core::Parallel
  RSpec.describe SpawnedWorker do
    describe ".environment" do
      it "tells a worker its number, the coordinator, the seed and its output files, and sets TEST_ENV_NUMBER" do
        env = SpawnedWorker.environment(2, 4242, 1234, :out => "/tmp/w.out", :err => "/tmp/w.err")

        expect(env).to eq("RSPEC_PARALLEL_WORKER" => "2", "RSPEC_PARALLEL_COORDINATOR" => "4242",
                          "RSPEC_PARALLEL_SEED" => "1234", "TEST_ENV_NUMBER" => "2",
                          "RSPEC_PARALLEL_STDOUT" => "/tmp/w.out", "RSPEC_PARALLEL_STDERR" => "/tmp/w.err")
        expect(SpawnedWorker.environment(1, 4242, 1234, nil)).to include("TEST_ENV_NUMBER" => "")
        expect(SpawnedWorker.environment(1, 4242, 1234, nil)).not_to have_key("RSPEC_PARALLEL_STDOUT")
      end
    end

    describe ".settings" do
      it "takes what the coordinator told it out of the environment, so that processes it starts are not workers" do
        env = SpawnedWorker.environment(3, 4242, 99, :out => "o", :err => "e").merge("OTHER" => "kept")

        expect(SpawnedWorker.settings(env)).to eq(:number => 3, :coordinator => 4242, :seed => 99,
                                                  :output => { :out => "o", :err => "e" })
        expect(env).to eq("TEST_ENV_NUMBER" => "3", "OTHER" => "kept")
      end

      it "has no output files when it was given none" do
        env = SpawnedWorker.environment(3, 4242, 99, nil)
        expect(SpawnedWorker.settings(env)[:output]).to be_nil
      end
    end

    describe ".configure" do
      let(:configuration) { RSpec::Core::Configuration.new }
      # Instead of this project's `.rspec` and the user's.
      let(:options_file) { Tempfile.new('rspec-options').tap(&:close).path }

      def configure(args, settings)
        SpawnedWorker.configure(['-O', options_file] + args, settings, configuration)
      end

      it "configures the worker from the command line, without the formatters and the files the coordinator writes" do
        configure(%w[--format json --out results.json --deprecation-out deprecations.txt --tag fast],
                  :number => 2, :seed => 1234)

        expect(configuration.formatters).to be_empty
        expect(configuration.deprecation_stream).not_to eq("deprecations.txt")
        expect(configuration.inclusion_filter.rules).to eq(:fast => true)
        expect(configuration.parallel_worker_number).to eq(2)
        expect(File.exist?("results.json")).to be false
      end

      it "takes the run's seed without choosing an ordering, which the run's configuration does" do
        configure(%w[--order defined], :number => 2, :seed => 1234)

        expect(configuration.seed).to eq(1234)
        expect(configuration.ordering_registry.fetch(:global)).to be_a(RSpec::Core::Ordering::Identity)
      end

      it "keeps a seed given on the command line, which is the run's too" do
        configure(%w[--seed 77], :number => 2, :seed => 77)

        expect(configuration.seed).to eq(77)
        expect(configuration.ordering_registry.fetch(:global)).to be_a(RSpec::Core::Ordering::Random)
      end
    end
  end

  RSpec.describe RSpec::Core::Ordering::ConfigurationManager do
    it "adopts a seed without changing the ordering, unless a seed was forced" do
      manager = RSpec::Core::Ordering::ConfigurationManager.new
      manager.adopt_seed(12)
      expect(manager.seed).to eq(12)
      expect(manager.seed_used?).to be false

      manager.force(:seed => 34)
      manager.adopt_seed(56)
      expect(manager.seed).to eq(34)
    end
  end
end
