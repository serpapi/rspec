module RSpec
  module Core
    module Parallel
      # Where Ruby has no `fork` (Windows, JRuby, TruffleRuby), a worker is a
      # new process running the `rspec` command the run was started with: it
      # configures itself from the same arguments, `.rspec` files and
      # `SPEC_OPTS` -- without formatters, which are the coordinator's -- and
      # the run's seed, then loads the spec files and goes on like a worker
      # forked before loading. Its jobs arrive on its standard input and its
      # results leave on its standard output, so what it prints goes to its
      # output files from the start. Configuration done in-process before
      # `RSpec::Core::Runner.run` cannot reach it, which is why only a run
      # started by the `rspec` command starts workers this way.
      # @private
      module SpawnedWorker
        VARIABLES = %w[
          RSPEC_PARALLEL_WORKER RSPEC_PARALLEL_COORDINATOR RSPEC_PARALLEL_SEED
          RSPEC_PARALLEL_STDOUT RSPEC_PARALLEL_STDERR
        ].freeze
        # Options that open files the coordinator writes to.
        NOT_FOR_WORKERS = [:formatters, :deprecation_stream].freeze

        # @return [Hash{String => String}] what a worker numbered `number` is
        #   told through its environment
        def self.environment(number, coordinator, seed, output)
          env = { 'RSPEC_PARALLEL_WORKER' => number.to_s, 'RSPEC_PARALLEL_COORDINATOR' => coordinator.to_s,
                  'RSPEC_PARALLEL_SEED' => seed.to_s, 'TEST_ENV_NUMBER' => Worker.test_env_number(number) }
          env.update('RSPEC_PARALLEL_STDOUT' => output[:out], 'RSPEC_PARALLEL_STDERR' => output[:err]) if output
          env
        end

        # Taken out of the environment, so that the processes it starts are
        # not workers.
        # @return [Hash]
        def self.settings(env=ENV)
          number, coordinator, seed, stdout, stderr = VARIABLES.map { |name| env.delete(name) }
          { :number => Integer(number), :coordinator => Integer(coordinator), :seed => seed && Integer(seed),
            :output => stdout && stderr && { :out => stdout, :err => stderr } }
        end

        def self.configure(args, settings, configuration=RSpec.configuration)
          configuration.parallel_worker_number = settings[:number]
          configuration.ordering_manager.adopt_seed(settings[:seed]) if settings[:seed]
          # Also for formatters its `--require`d files add.
          configuration.formatter_loader = Worker::FormatterLoader.new(Reporter.new(configuration))
          options = ConfigurationOptions.new(args)
          NOT_FOR_WORKERS.each { |key| options.options.delete(key) }
          options.configure(configuration)
        end

        # Never returns.
        # :nocov: - runs in the spawned worker, covered by integration specs
        def self.run(args)
          settings = settings()
          # rubocop:disable Style/GlobalStdStream
          # The process's own streams, which the coordinator connected.
          jobs = Channel::Reader.new(STDIN.dup)
          results = Channel::Writer.new(STDOUT.dup)
          STDIN.reopen(File::NULL)
          # Its standard output must not be where its results go.
          STDOUT.reopen(STDERR) unless settings[:output]
          # rubocop:enable Style/GlobalStdStream
          Runner.disable_autorun!
          loading = Worker::LoadReporter.new(RSpec.configuration)
          worker = Worker.new(settings[:number], nil, jobs, results,
                              :output => settings[:output], :coordinator => settings[:coordinator], :loading => loading)
          worker.capture_output
          worker.report_started
          RSpec.configuration.reporter = loading
          configure(args, settings)
          worker.run
        end
        # :nocov:
      end
    end
  end
end
