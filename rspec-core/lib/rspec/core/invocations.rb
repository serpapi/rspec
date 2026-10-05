module RSpec
  module Core
    # @private
    module Invocations
      # @private
      class InitializeProject
        def call(*_args)
          RSpec::Support.require_rspec_core "project_initializer"
          ProjectInitializer.new.run
          0
        end
      end

      # @private
      class DRbWithFallback
        def call(options, err, out)
          require 'rspec/core/drb'
          begin
            return DRbRunner.new(options).run(err, out)
          rescue DRb::DRbConnError
            err.puts "No DRb server is running. Running in local process instead ..."
          end
          RSpec::Core::Runner.new(options).run(err, out)
        end
      end

      # @private
      class Bisect
        def call(options, err, out)
          RSpec::Support.require_rspec_core "bisect/coordinator"
          runner = Runner.new(options).tap { |r| r.configure(err, out) }
          formatter = bisect_formatter_klass_for(options.options[:bisect]).new(
            out, runner.configuration.bisect_runner
          )

          success = without_parallel_spec_opts do
            RSpec::Core::Bisect::Coordinator.bisect_with(runner, cli_args_for(options), formatter)
          end

          runner.exit_code(success)
        end

      private

        PARALLEL_OPTION = /\A--(?:no-)?parallel(?:=|\z)/

        # Bisection runs the suite serially, and so does the reproduction
        # command it prints -- even when parallel runs are enabled where this
        # process cannot see it, in a helper that the spec files require.
        def cli_args_for(options)
          options.args.reject { |arg| arg =~ PARALLEL_OPTION } + ['--no-parallel']
        end

        # `SPEC_OPTS` win over the command line, and the reproduction command
        # and the runs of `--bisect-runner=shell` reuse them.
        def without_parallel_spec_opts
          original = ENV.fetch('SPEC_OPTS', nil)
          if original
            require 'shellwords'
            args = Shellwords.split(original)
            remaining = args.reject { |arg| arg =~ PARALLEL_OPTION }
            ENV['SPEC_OPTS'] = remaining.empty? ? nil : Shellwords.join(remaining) unless remaining == args
          end
          yield
        ensure
          ENV['SPEC_OPTS'] = original
        end

        def bisect_formatter_klass_for(argument)
          return Formatters::BisectDebugFormatter if argument == "verbose"
          Formatters::BisectProgressFormatter
        end
      end

      # @private
      class PrintVersion
        def call(_options, _err, out)
          overall_version = RSpec::Core::Version::STRING
          unless overall_version =~ /[a-zA-Z]+/
            overall_version = overall_version.split('.').first(2).join('.')
          end

          out.puts "RSpec #{overall_version}"

          [:Core, :Expectations, :Mocks, :Rails, :Support].each do |const_name|
            lib_name = const_name.to_s.downcase
            begin
              require "rspec/#{lib_name}/version"
            rescue LoadError
              # Not worth mentioning libs that are not installed
              nil
            else
              out.puts "  - rspec-#{lib_name} #{RSpec.const_get(const_name)::Version::STRING}"
            end
          end

          0
        end
      end

      # @private
      PrintHelp = Struct.new(:parser, :hidden_options) do
        def call(_options, _err, out)
          # Removing the hidden options from the output.
          out.puts parser.to_s.gsub(/^\s+(#{hidden_options.join('|')})\b.*$\n/, '')
          0
        end
      end
    end
  end
end
