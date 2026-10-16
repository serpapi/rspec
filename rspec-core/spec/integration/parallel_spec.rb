require 'rspec/core/parallel/coordinator'
require 'support/aruba_support'
require 'json'

# Runs `rspec` in a process of its own, where the `--require`d files load
# afresh. Given at most 60 seconds.
module ParallelRunProcess
  def rspec_process(*args)
    cd('.') do
      rspec = [RbConfig.ruby, "-I#{$LOAD_PATH.join(File::PATH_SEPARATOR)}", File.expand_path('../../exe/rspec', __dir__)]
      pid = Process.spawn({ 'TEST_ENV_NUMBER' => nil }, *rspec, *args, :out => 'stdout.txt', :err => 'stderr.txt')
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 60
      sleep 0.05 until (status = Process.waitpid2(pid, Process::WNOHANG)&.last) ||
                       Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      Process.kill(:KILL, pid) && Process.wait(pid) unless status
      [status, File.read('stdout.txt'), File.read('stderr.txt')]
    end
  end
end

# The longest group of rspec-core's suite: under `--parallel` it runs as parts in
# several workers (each has its own aruba directory).
RSpec.describe 'Parallel execution (--parallel)', :slow, :parallel_split => 4,
               :skip => !RSpec::Support::RubyFeatures.fork_supported? do
  include_context "aruba support"
  include ParallelRunProcess

  # The coordinator refuses to nest inside another parallel worker, which is
  # what this very process is when rspec-core's suite runs under `--parallel`.
  # Aruba swaps in its own environment while a command runs, so the variable
  # is removed from (or, for the fallback test, added to) that environment.
  def run_rspec(cmd)
    if @inside_another_parallel_worker
      set_environment_variable("TEST_ENV_NUMBER", "2")
    else
      delete_environment_variable("TEST_ENV_NUMBER")
    end
    run_command(cmd)
  end

  # Written by the workers' `before(:suite)` hooks: a file can exist before
  # its pid has been written to it.
  def worker_pids
    Dir['worker_*.pid'].map { |file| File.read(file) }.reject(&:empty?).map { |pid| Integer(pid) }
  end

  def write_fixed_suite
    write_file_formatted 'spec/alpha_spec.rb', "
      RSpec.describe 'Alpha' do
        it('passes') { expect(1).to eq(1) }
        it('fails') { expect(1).to eq(2) }
        it('is pending') { pending 'not yet'; expect(1).to eq(2) }
        it('is skipped') { skip 'later' }
        context 'nested' do
          it('fails with a cause') { begin; raise 'root'; rescue; raise ArgumentError, 'wrapped'; end }
        end
      end
    "

    write_file_formatted 'spec/beta_spec.rb', "
      RSpec.describe 'Beta' do
        it('aggregates', :aggregate_failures) { expect(1).to eq(2); expect(2).to eq(3) }
        it('passes too') { expect(true).to be(true) }
        it('fails in an after hook') { }
        after { |ex| raise 'in after' if ex.description == 'fails in an after hook' }
      end
    "

    write_file_formatted 'spec/gamma_spec.rb', "
      RSpec.describe 'Gamma' do
        3.times { |i| it(\"passes \#{i}\") { expect(i).to be >= 0 } }
      end
    "

    # Described by their matchers once they have run.
    write_file_formatted 'spec/delta_spec.rb', "
      RSpec.describe 'Delta' do
        subject { 5 }
        it { is_expected.to eq(5) }
        it { is_expected.to eq(6) }
      end
    "
  end

  # Timing differs between runs, and failures are numbered in the order they
  # are reported, which for a parallel run is the order groups finish in.
  # Frames below the spec files differ too because these specs run RSpec
  # inside RSpec (the worker's stack goes through the coordinator's fork).
  def normalize(output)
    output.
      gsub(/Finished in .*$/, 'Finished in X').
      gsub(/\d+\.\d+ seconds/, 'X seconds').
      gsub(/\d+(\.\d+)? seconds? average.*$/, 'X average').
      gsub(/\(FAILED - \d+\)/, '(FAILED - N)').
      gsub(/^(\s*)\d+(\.\d+)?\) /, '\\1N) ').
      lines.reject { |line| line =~ /^\s*# / && line !~ %r{# \./spec/} }.join
  end

  def group_blocks(documentation_output)
    body = documentation_output.split(/^Failures:$|^Pending:/).first
    body.split(/\n(?=\S)/).map(&:strip).reject(&:empty?).reject { |block| block.start_with?("Randomized") }
  end

  it 'runs the groups across workers and reports one coherent run with the serial exit code' do
    write_fixed_suite

    run_rspec "--parallel=2 --order defined"

    expect(last_cmd_exit_status).to eq(1)
    expect(last_cmd_stdout).to include("13 examples, 5 failures, 2 pending")
    expect(last_cmd_stdout).to include("Alpha fails", "Alpha nested fails with a cause", "Beta aggregates", "Beta fails in an after hook",
                                       "rspec ./spec/delta_spec.rb:4 # Delta is expected to eq 6")
    expect(last_cmd_stdout).to include("Got 2 failures:", "--- Caused by: ---", "root")
    expect(last_cmd_stdout.scan("Randomized with seed").size).to eq(0)
    expect(last_cmd_stderr).to eq("")
  end

  it 'gives each worker its number and a parallel_tests-style TEST_ENV_NUMBER, and runs suite hooks once per worker' do
    write_file_formatted 'spec/env_spec.rb', "
      RSpec.configure do |c|
        c.before(:suite) { File.write(\"before_suite_\#{RSpec.configuration.parallel_worker_number}\", ENV['TEST_ENV_NUMBER'].inspect) }
        c.after(:suite) { File.write(\"after_suite_\#{RSpec.configuration.parallel_worker_number}\", Process.pid) }
      end
      3.times do |i|
        RSpec.describe(\"G\#{i}\") do
          it('records its worker') { File.write(\"example_\#{i}\", RSpec.configuration.parallel_worker_number.to_s) }
        end
      end
    "

    test_env_number_before = ENV['TEST_ENV_NUMBER']
    run_rspec "--parallel=2 spec/env_spec.rb"

    expect(last_cmd_exit_status).to eq(0)
    expect(last_cmd_stdout).to include("3 examples, 0 failures")
    cd '.' do
      expect(File.read("before_suite_1")).to eq('""')
      expect(File.read("before_suite_2")).to eq('"2"')
      expect(File.exist?("after_suite_1")).to be true
      expect(File.exist?("after_suite_2")).to be true
      expect(Dir["example_*"].map { |f| File.read(f) }.sort).to all(match(/\A[12]\z/))
    end
    expect(RSpec.configuration.parallel_worker_number).to be_nil
    expect(ENV['TEST_ENV_NUMBER']).to eq(test_env_number_before)
  end

  it 'notifies listeners registered in a before(:suite) hook and shows the failure lines they add, like a serial run' do
    write_file_formatted 'spec/listener_spec.rb', "
      class RefListener
        def example_failed(notification)
          notification.example.metadata[:extra_failure_lines] = ['Ref: added by a before(:suite) listener']
        end
      end
      RSpec.configure do |c|
        c.before(:suite) { RSpec.configuration.reporter.register_listener(RefListener.new, :example_failed) }
      end
      RSpec.describe('One') { it('fails') { expect(1).to eq(2) } }
      RSpec.describe('Two') { it('fails too') { expect(2).to eq(3) } }
    "

    run_rspec "--order defined spec/listener_spec.rb"
    serial_stdout = last_cmd_stdout
    run_rspec "--parallel=2 --order defined spec/listener_spec.rb"

    expect(last_cmd_exit_status).to eq(1)
    expect(serial_stdout.scan('Ref: added by a before(:suite) listener').size).to eq(2)
    expect(last_cmd_stdout.scan('Ref: added by a before(:suite) listener').size).to eq(2)
  end

  describe 'formatter parity with a serial run' do
    before { write_fixed_suite }

    it 'produces the same progress output' do
      run_rspec "--order defined --format progress"
      serial = normalize(last_cmd_stdout)
      run_rspec "--order defined --format progress --parallel=2"
      parallel = normalize(last_cmd_stdout)

      serial_dots, serial_rest = serial.split("\n", 2)
      parallel_dots, parallel_rest = parallel.split("\n", 2)
      expect(parallel_dots.chars.sort).to eq(serial_dots.chars.sort)
      expect(parallel_rest.lines.sort).to eq(serial_rest.lines.sort)
    end

    it 'produces the same documentation blocks for every group, with the serial order inside each group' do
      run_rspec "--order defined --format documentation"
      serial = normalize(last_cmd_stdout)
      run_rspec "--order defined --format documentation --parallel=3"
      parallel = normalize(last_cmd_stdout)

      expect(group_blocks(parallel)).to match_array(group_blocks(serial))
      expect(parallel.lines.sort).to eq(serial.lines.sort)
    end

    it 'produces the same JSON' do
      run_rspec "--order defined --format json --out serial.json"
      run_rspec "--order defined --format json --out parallel.json --parallel=2"

      cd '.' do
        serial, parallel = %w[serial.json parallel.json].map do |file|
          json = JSON.parse(File.read(file))
          json.delete("summary_line")
          json["summary"].delete("duration")
          json["examples"].each do |e|
            e.delete("run_time")
            e["exception"]["backtrace"].select! { |frame| frame.start_with?("./spec/") } if e["exception"]
          end
          json["examples"] = json["examples"].sort_by { |e| e["id"] }
          json
        end

        expect(parallel).to eq(serial)
        expect(parallel["examples"].select { |e| e["exception"] }.map { |e| e["exception"]["class"] }).to include(
          "RSpec::Expectations::ExpectationNotMetError", "ArgumentError", "RSpec::Expectations::MultipleExpectationsNotMetError", "RuntimeError"
        )
      end
    end

    it 'produces the same failures list' do
      run_rspec "--order defined --format failures"
      serial = last_cmd_stdout
      run_rspec "--order defined --format failures --parallel=2"

      expect(last_cmd_stdout.lines.sort).to eq(serial.lines.sort)
    end

    it 'produces structurally equivalent HTML' do
      run_rspec "--order defined --format html --out serial.html"
      run_rspec "--order defined --format html --out parallel.html --parallel=2"

      cd '.' do
        serial, parallel = %w[serial.html parallel.html].map { |file| File.read(file) }
        expect(parallel.scan(/class="example (\w+)"/).sort).to eq(serial.scan(/class="example (\w+)"/).sort)
        expect(parallel.scan(/<dl[^>]*>/).size).to eq(serial.scan(/<dl[^>]*>/).size)
        expect(parallel.scan('<span class="duration">').size).to eq(serial.scan('<span class="duration">').size)
        expect(parallel).to include("13 examples, 5 failures, 2 pending")
      end
    end

    it 'merges the profile with the durations measured in the workers' do
      run_rspec "--order defined --profile 2 --parallel=2"

      expect(last_cmd_stdout).to include("Top 2 slowest examples", "Top 2 slowest example groups")
      expect(last_cmd_stdout).to match(/\d+\.\d+ seconds average \(\d+\.\d+ seconds \/ \d+ examples?\) \.\/spec\/\w+_spec\.rb:1/)
    end
  end

  # As rspec-expectations does for every expectation when its reporter counts
  # them (rspec/rspec#305), the examples here call the configured reporter's
  # `increment_expectation_count`, which the formatter adds to the reporter
  # of the coordinating process (or of the serial run).
  it 'adds up the expectations its workers count, as a serial run does, for a reporter that counts them' do
    write_file_formatted 'spec/support/counted_expectations.rb', "
      class CountedExpectations
        RSpec::Core::Formatters.register self, :start, :dump_summary

        def initialize(output)
          @output = output
        end

        def start(_notification)
          counted = 0
          reporter = RSpec.configuration.reporter
          reporter.define_singleton_method(:increment_expectation_count) { counted += 1 }
          reporter.define_singleton_method(:counted_expectations) { counted }
        end

        def dump_summary(_notification)
          @output.puts \"Counted \#{RSpec.configuration.reporter.counted_expectations} expectations\"
        end
      end
    "
    4.times do |i|
      write_file_formatted "spec/c#{i}_spec.rb", "
        RSpec.describe('C#{i}') do
          before(:context) { RSpec.configuration.reporter.increment_expectation_count }
          it('counts twice') { 2.times { RSpec.configuration.reporter.increment_expectation_count } }
          it('counts once') { RSpec.configuration.reporter.increment_expectation_count }
        end
      "
    end
    options = '--require ./spec/support/counted_expectations --format progress --format CountedExpectations'

    run_rspec options
    expect(last_cmd_stdout).to include("8 examples, 0 failures", "Counted 16 expectations")

    run_rspec "#{options} --parallel=2"
    expect(last_cmd_stdout).to include("8 examples, 0 failures", "Counted 16 expectations")
    expect(last_cmd_exit_status).to eq(0)
  end

  it 'shows formatters the metadata an example set, changed or deleted in a worker, as a serial run does' do
    write_file_formatted 'spec/support/metadata_formatter.rb', "
      class MetadataFormatter
        RSpec::Core::Formatters.register self, :example_passed

        def initialize(output)
          @output = output
        end

        def example_passed(notification)
          metadata = notification.example.metadata
          @output.puts \"META \#{notification.example.description}: \#{metadata[:screenshot].inspect} \" \\
                       \"\#{metadata[:tags].inspect} \#{metadata.key?(:gone)} \#{metadata[:callback].class}\"
        end
      end
    "
    4.times do |i|
      write_file_formatted "spec/m#{i}_spec.rb", "
        RSpec.describe('M#{i}', :gone => true) do
          it('e#{i}', :tags => ['a']) do |ex|
            ex.metadata[:screenshot] = 'shot-#{i}.png'
            ex.metadata[:tags] << 'b'
            ex.metadata.delete(:gone)
          end
          it('callback#{i}') { |ex| ex.metadata[:callback] = lambda {} }
        end
      "
    end
    options = '--require ./spec/support/metadata_formatter --format progress --format MetadataFormatter'

    run_rspec options
    serial = last_cmd_stdout.scan(/META .*$/).sort
    expect(serial.size).to eq(8)
    expect(serial.first).to eq("META callback0: nil nil true Proc")
    expect(serial).to include("META e3: \"shot-3.png\" [\"a\", \"b\"] false NilClass")

    run_rspec "#{options} --parallel=2"
    parallel = last_cmd_stdout.scan(/META .*$/).sort
    expect(parallel).to eq(serial.map { |line| line.sub(/ Proc$/, ' NilClass') })
    expect(last_cmd_stderr.scan("set example metadata :callback to a Proc").size).to eq(1)
    expect(last_cmd_exit_status).to eq(0)
  end

  it 'shows what workers print with the example group that printed it, as a serial run does' do
    write_file_formatted 'spec/support/at_exit.rb', "
      RSpec.configure { |c| c.after(:suite) { at_exit { puts \"at_exit of worker \#{c.parallel_worker_number}\" } } }
    "
    3.times do |i|
      write_file_formatted "spec/p#{i}_spec.rb", "
        puts 'loading p#{i}'
        RSpec.describe('P#{i}') do
          it('prints') do
            puts 'stdout of P#{i}'
            warn 'stderr of P#{i}'
            system('echo subprocess of P#{i}')
          end
          it('is quiet') { }
        end
      "
    end
    write_file_formatted 'spec/killed_spec.rb', "
      RSpec.describe('Killed') do
        it('says goodbye') { $stderr.puts 'last words'; Process.kill(:KILL, Process.pid) }
      end
    "
    options = %w[--require ./spec/support/at_exit --format documentation --order defined --parallel=2]

    status, stdout, stderr = rspec_process(*options)
    # The coordinator says so when it replaces the killed worker.
    blocks = stdout.gsub(/^ *Parallel worker \d \(pid \d+\) was started in place .*\n/, '').split(/^(?=P\d$)/)
    blocks = blocks.select { |block| block =~ /\AP\d$/ }
    expect(blocks.size).to eq(3)
    blocks.each do |block|
      name = block[/\AP\d/]
      expect(block).to start_with("#{name}\nstdout of #{name}\nsubprocess of #{name}\n  prints\n  is quiet\n")
    end
    # The worker killed with SIGKILL runs no `at_exit` handler.
    expect(stdout.scan(/^at_exit of worker \d$/)).not_to be_empty
    expect(stderr).to include("stderr of P0\n", "stderr of P1\n", "stderr of P2\n", "last words\n")
    expect(stdout).to include("7 examples, 1 failure")
    expect(status.exitstatus).to eq(1)

    status, stdout, _stderr = rspec_process(*options, '--no-parallel-preload', *%w[spec/p0_spec.rb spec/p1_spec.rb spec/p2_spec.rb])
    expect(stdout.scan(/^loading p\d$/)).to eq(["loading p0", "loading p1", "loading p2"])
    expect(stdout).to include("6 examples, 0 failures")
    expect(status.exitstatus).to eq(0)
  end

  it 'starts a worker with the same TEST_ENV_NUMBER in place of one that crashed, which runs what was left' do
    4.times do |i|
      write_file_formatted "spec/r#{i}_spec.rb", "
        RSpec.describe('R#{i}') do
          it('records') do
            File.write('ran_r#{i}', [Process.pid, ENV['TEST_ENV_NUMBER'].inspect].join(' '))
            Process.kill(:KILL, Process.pid) if #{i} == 0
            sleep 1 if #{i} == 1
          end
        end
      "
    end

    run_rspec '--parallel=2 --order defined'

    expect(last_cmd_stdout).to include("4 examples, 1 failure", "exited unexpectedly", "was started in place of the lost worker")
    expect(last_cmd_exit_status).to eq(1)
    ran = cd('.') { Dir['ran_r*'].to_h { |file| [file, File.read(file).split] } }
    expect(ran.keys).to eq(%w[ran_r0 ran_r1 ran_r2 ran_r3])
    crashed_pid, crashed_env = ran['ran_r0']
    replacement = ran.values.find { |pid, env| env == crashed_env && pid != crashed_pid }
    expect(replacement).not_to be_nil
  end

  it 'lets a custom formatter use metadata, exception classes, custom notifications and group boundaries' do
    write_file_formatted 'spec/support/my_formatter.rb', "
      class MyAppError < StandardError; end

      class MyFormatter
        RSpec::Core::Formatters.register self, :example_group_started, :example_group_finished, :example_failed, :example_passed, :my_event, :message

        def initialize(output)
          @output = output
          @depth = 0
        end

        def example_group_started(notification)
          @output.puts \"\#{'  ' * @depth}> \#{notification.group.description} (\#{notification.group.metadata[:kind]})\"
          @depth += 1
        end

        def example_group_finished(notification)
          @depth -= 1
          @output.puts \"\#{'  ' * @depth}< \#{notification.group.description}\"
        end

        def example_passed(notification)
          result = notification.example.execution_result
          timed = result.started_at.is_a?(Time) && result.finished_at.is_a?(Time) && result.finished_at >= result.started_at
          @output.puts \"\#{'  ' * @depth}ok \#{notification.example.metadata[:flag].inspect} timed=\#{timed}\"
        end

        def example_failed(notification)
          e = notification.exception
          @output.puts \"\#{'  ' * @depth}FAILED \#{e.class.name} app_error=\#{e.is_a?(MyAppError)} std=\#{e.is_a?(StandardError)} msg=\#{e.message}\"
          @output.puts notification.fully_formatted(1)
        end

        def my_event(notification)
          @output.puts \"custom: \#{notification.payload.inspect}\"
        end

        def message(notification)
          @output.puts \"message: \#{notification.message}\"
        end
      end
    "

    write_file_formatted 'spec/custom_spec.rb', "
      RSpec.describe 'One', :kind => :unit do
        it('passes', :flag => 7) { RSpec.configuration.reporter.publish(:my_event, :payload => { :from => 'worker' }) }
        it('raises an app error') { raise MyAppError, 'app broke' }
        it('publishes something that cannot be marshaled') { RSpec.configuration.reporter.publish(:my_event, :payload => proc {}) }
      end

      RSpec.describe 'Two', :kind => :feature do
        it('passes') { }
      end
    "

    run_rspec "--order defined -r ./spec/support/my_formatter --format MyFormatter --parallel=2 spec/custom_spec.rb"

    expect(last_cmd_stdout).to include(
      "> One (unit)",
      "  ok 7 timed=true",
      "  FAILED MyAppError app_error=true std=true msg=app broke",
      "Failure/Error: it('raises an app error') { raise MyAppError, 'app broke' }",
      "< One",
      "> Two (feature)",
      "  ok nil timed=true",
      "< Two",
      "custom: #{{ :from => 'worker' }.inspect}",
      "message: Parallel worker 1 could not forward the custom `my_event` notification because its options cannot be " \
      "marshaled: #{{ :payload => 0 }.inspect.chomp('0}')}#<Proc"
    )
    expect(last_cmd_exit_status).to eq(1)
  end

  it 'honours --fail-fast across workers by asking them to stop between examples' do
    write_file_formatted 'spec/fast_fail_spec.rb', "
      RSpec.describe 'Fails first' do
        it('fails') { File.write('failed', ''); expect(1).to eq(2) }
      end
    "
    write_file_formatted 'spec/slow_spec.rb', "
      RSpec.describe 'Slow' do
        it('waits for the failure') do
          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
          sleep 0.01 until File.exist?('failed') || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        end
        10.times { |i| it(\"example \#{i}\") { sleep 0.15 } }
      end
    "

    run_rspec "--order defined --fail-fast --parallel=2"

    expect(last_cmd_exit_status).to eq(1)
    # Later segments of the summary line (pending, errors, ...) are not this spec's business.
    expect(last_cmd_stdout).to match(/^\d+ examples?, 1 failure(, .+)?$/)
    expect(last_cmd_stdout[/^(\d+) examples?/, 1].to_i).to be < 12
  end

  it 'persists example statuses from every worker and reruns only the failures with --only-failures' do
    write_fixed_suite

    RSpec.configuration.example_status_persistence_file_path = 'statuses.txt'
    run_rspec "--order defined --parallel=2"
    expect(last_cmd_stdout).to include("13 examples, 5 failures, 2 pending")
    cd '.' do
      statuses = File.read("statuses.txt")
      expect(statuses.scan('| passed ').size).to eq(6)
      expect(statuses.scan('| failed ').size).to eq(5)
      expect(statuses.scan('| pending').size).to eq(2)
      expect(statuses).to match(/\d+\.\d+ seconds/)
    end

    RSpec.configuration.example_status_persistence_file_path = 'statuses.txt'
    run_rspec "--order defined --parallel=2 --only-failures"
    expect(last_cmd_stdout).to include("5 examples, 5 failures")
  end

  it 'forwards deprecations and messages from the workers into the single report' do
    write_file_formatted 'spec/deprecation_spec.rb', "
      RSpec.configure { |c| c.before(:suite) { RSpec.configuration.reporter.message(\"hello from worker \#{RSpec.configuration.parallel_worker_number}\") } }
      RSpec.describe('A') { it('deprecates') { RSpec.deprecate('old_thing', :replacement => 'new_thing') } }
      RSpec.describe('B') { it('deprecates too') { RSpec.deprecate('other_thing') } }
    "

    run_rspec "--parallel=2 --deprecation-out deprecations.txt spec/deprecation_spec.rb"

    expect(last_cmd_exit_status).to eq(0)
    expect(last_cmd_stdout).to include("hello from worker 1", "hello from worker 2")
    expect(last_cmd_stdout).to include("2 deprecations logged to deprecations.txt")
    cd '.' do
      deprecations = File.read("deprecations.txt")
      expect(deprecations).to include("old_thing is deprecated. Use new_thing instead.", "other_thing is deprecated.")
    end
  end

  it 'reports suite hook failures from workers like a serial run does, with the error exit code' do
    write_file_formatted 'spec/suite_hook_spec.rb', "
      RSpec.configure do |c|
        c.after(:suite) { raise 'cleanup broke' if RSpec.configuration.parallel_worker_number == 2 }
      end
      RSpec.describe('A') { it('passes') { } }
      RSpec.describe('B') { it('passes') { } }
    "

    run_rspec "--parallel=2 --error-exit-code 3 spec/suite_hook_spec.rb"

    expect(last_cmd_exit_status).to eq(3)
    expect(last_cmd_stdout).to include("An error occurred in an `after(:suite)` hook.", "cleanup broke")
    expect(last_cmd_stdout).to include("2 examples, 0 failures, 1 error occurred outside of examples")
  end

  it 'does not replay output buffered before the fork into files written by formatters' do
    write_fixed_suite

    run_rspec "--order defined --format documentation --out doc.txt --parallel=3 spec/gamma_spec.rb spec/beta_spec.rb"

    cd '.' do
      expect(File.read("doc.txt").scan("Randomized with seed").size).to eq(0)
      expect(File.read("doc.txt").scan(/^Gamma$/).size).to eq(1)
    end
  end

  describe 'a worker whose own world wants to quit (an interrupt delivered to that worker only)' do
    def write_self_interrupting_group(name, examples_after=2)
      write_file_formatted "spec/#{name.downcase}_spec.rb", "
        RSpec.describe '#{name}' do
          it('interrupts itself') { Process.kill('INT', Process.pid); sleep 0.2 }
          #{examples_after}.times { |i| it(\"never runs \#{i}\") { } }
        end
      "
    end

    it 'retires that worker after the group it was running and lets the others finish, failing the run' do
      write_self_interrupting_group('Interrupted')
      write_file_formatted 'spec/other_spec.rb', "
        RSpec.describe('Other') { 2.times { |i| it(\"passes \#{i}\") { sleep 0.05 } } }
      "
      write_file_formatted 'spec/third_spec.rb', "
        RSpec.describe('Third') { it('passes') { } }
      "

      run_rspec "--order defined --parallel=2"

      expect(last_cmd_exit_status).to eq(1)
      expect(last_cmd_stdout).to match(/Parallel worker \d \(pid \d+\) stopped after Interrupted .* because `RSpec.world.wants_to_quit` was set/)
      expect(last_cmd_stdout).to include("4 examples, 0 failures")
      expect(last_cmd_stdout).not_to include("RSpec is shutting down")
    end

    it 'reports the groups nobody was left to run when every worker retires' do
      %w[One Two Three Four].each { |name| write_self_interrupting_group(name, 0) }

      run_rspec "--order defined --parallel=2"

      expect(last_cmd_exit_status).to eq(1)
      expect(last_cmd_stdout).to include("2 examples, 0 failures")
      expect(last_cmd_stdout).to match(
        /^2 examples of 2 example groups did not run because no worker remained to run them\. To run them: rspec '?\.\/spec\/\w+_spec\.rb\[1\]'? '?\.\/spec\/\w+_spec\.rb\[1\]'?$/
      )
    end
  end

  it 'fails the example a worker dies in, with the reason, so it can be rerun, and runs the rest of its group in another worker' do
    write_file_formatted 'spec/crash_spec.rb', "
      RSpec.configure { |c| c.example_status_persistence_file_path = 'status.txt' }
      RSpec.describe 'Crashing' do
        it('finishes') { }
        it('dies') { File.write(\"dies_\#{Process.pid}\", ''); exit!(7) }
        it('runs in another worker') { File.write(\"rest_\#{Process.pid}\", '') }
      end
      RSpec.describe('Fine') { it('passes') { } }
    "

    run_rspec "--order defined --parallel=2 --error-exit-code 3 spec/crash_spec.rb"

    expect(last_cmd_exit_status).to eq(1)
    expect(last_cmd_stdout).to match(
      /Failure\/Error: it\('dies'\) \{ File\.write.*exit!\(7\) \}\n\s+Parallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 7\) while running this example; 1 example of Crashing \(\.\/spec\/crash_spec\.rb:2\) that it had not started yet will run in another worker\./
    )
    expect(last_cmd_stdout).to include("4 examples, 1 failure", "rspec ./spec/crash_spec.rb:4 # Crashing dies")
    cd('.') do
      expect(File.read('status.txt')).to match(%r{^\./spec/crash_spec\.rb\[1:2\] \| failed })
      expect(File.read('status.txt')).to match(%r{^\./spec/crash_spec\.rb\[1:3\] \| passed })
      expect(Dir['dies_*'].size).to eq(1)
      expect(Dir['rest_*'].map { |file| file[/\d+/] }).not_to include(Dir['dies_*'].first[/\d+/])
    end
  end

  it 'fails the examples of a group whose context hook a worker dies in, as a raising hook would, and runs the rest elsewhere' do
    write_file_formatted 'spec/crash_spec.rb', "
      RSpec.configure { |c| c.example_status_persistence_file_path = 'status.txt' }
      RSpec.describe 'Crashing' do
        context 'broken' do
          before(:context) { exit!(7) }
          it('cannot run') { }
        end
        context 'fine' do
          it('runs in another worker') { }
        end
      end
      RSpec.describe('Fine') { it('passes') { sleep 0.5 } }
    "

    run_rspec "--order defined --parallel=2 --error-exit-code 3 spec/crash_spec.rb"

    expect(last_cmd_exit_status).to eq(1)
    expect(last_cmd_stdout).to match(
      /Parallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 7\) while running Crashing broken \(\.\/spec\/crash_spec\.rb:3\), outside of any example, so this example could not run; 1 example of Crashing \(\.\/spec\/crash_spec\.rb:2\) that it had not started yet will run in another worker\./
    )
    expect(last_cmd_stdout).to include("3 examples, 1 failure", "rspec ./spec/crash_spec.rb:5 # Crashing broken cannot run")
    expect(last_cmd_stdout).not_to include("outside of examples")
    cd('.') do
      expect(File.read('status.txt')).to match(%r{^\./spec/crash_spec\.rb\[1:1:1\] \| failed })
      expect(File.read('status.txt')).to match(%r{^\./spec/crash_spec\.rb\[1:2:1\] \| passed })
    end
  end

  describe 'when the code an example runs calls `exit`' do
    # A run that a worker's `exit` ends raises `SystemExit` out of
    # `Runner.run`, as a serial run does -- which would end this very run.
    def run_rspec(cmd)
      super
    rescue SystemExit => e
      @last_cmd_exit_status = e.status
    end

    def write_exiting_suite(status, after_suite="")
      write_file_formatted 'spec/exit_spec.rb', "
        RSpec.configure do |c|
          c.parallel_shutdown_timeout = 0.5
          c.after(:suite) { #{after_suite} }
        end
        RSpec.describe 'Exits' do
          it('exits') do
            sleep 0.2
            $exiting = true
            exit #{status}
          end
          it('never runs') { File.write('ran_after_exit', '') }
        end
        RSpec.describe 'Keeps going' do
          20.times { |i| it(\"waits \#{i}\") { sleep 0.05 } }
        end
      "
    end

    [0, 3].each do |status|
      it "ends the run as a serial run does, with status #{status}: no further examples start" do
        write_exiting_suite(status)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        run_rspec "--order defined --parallel=2"

        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 10
        expect(last_cmd_exit_status).to eq(status)
        expect(last_cmd_stdout).to match(
          /Parallel worker \d \(pid \d+\) is ending the run: the code it ran called `exit` or `abort` \(status #{status}\) in Exits exits \(\.\/spec\/exit_spec\.rb:6\)\. As in a serial run, no further examples start and the run exits with status #{status}\.$/
        )
        expect(last_cmd_stdout[/^(\d+) examples?/, 1].to_i).to be < 22
        expect(last_cmd_stdout).to match(/^\d+ examples?, 0 failures(, .+)?$/)
        cd('.') { expect(File).not_to exist('ran_after_exit') }
      end
    end

    it "ends with the status of an `after(:suite)` hook's later exit, as a serial run does" do
      write_exiting_suite(4, "exit 5 if $exiting")

      run_rspec "--order defined --parallel=2"

      expect(last_cmd_exit_status).to eq(5)
      expect(last_cmd_stdout).to match(
        /Parallel worker \d \(pid \d+\): then an `after\(:suite\)` hook called `exit` or `abort` \(status 5\), so the run exits with status 5\./
      )
      expect(last_cmd_stdout).not_to include("An error occurred in a parallel worker")
    end

    it 'fails the run as usual when another worker then has to be stopped by force' do
      write_file_formatted 'spec/exit_spec.rb', "
        RSpec.configure { |c| c.parallel_shutdown_timeout = 0.5 }
        RSpec.describe('Exits') { it('exits') { sleep 0.5; exit 0 } }
        RSpec.describe('Stuck') { it('sleeps') { sleep 30 } }
      "
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--order defined --parallel=2"

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 15
      expect(last_cmd_exit_status).to eq(1)
      expect(last_cmd_stdout).to include(
        "is ending the run: the code it ran called `exit` or `abort` (status 0)",
        "did not stop within 0.5 seconds of being asked to stop early (fail-fast or interrupt) while running this example"
      )
    end

    it 'is bounded when the worker then does not exit, failing the run' do
      # Leaves the worker's watchdog plenty of room to fire before the
      # coordinator steps in, even on a loaded machine.
      stub_const("RSpec::Core::Parallel::Coordinator::WATCHDOG_GRACE", 5.0)
      write_exiting_suite(0, "if $exiting; $stderr = File.open('worker_stderr.txt', 'w'); sleep 30; end")
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--order defined --parallel=2 --error-exit-code 3"

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 10
      expect(last_cmd_exit_status).to eq(3)
      expect(last_cmd_stdout).to match(
        /Parallel worker \d \(pid \d+\) ended the run because the code it ran called `exit` or `abort` \(status 0\), then exited with exit status 1: if its watchdog aborted it, its `after\(:suite\)` hooks or `at_exit` handlers did not finish within 0\.5 seconds/
      )
      expect(cd('.') { File.read('worker_stderr.txt') }).to include(
        "did not exit within 0.5 seconds of stopping because the code it ran called `exit` or `abort` (status 0); aborting it."
      )
    end

    it 'is bounded by the coordinator when the worker cannot abort itself and ignores SIGTERM' do
      stub_const("RSpec::Core::Parallel::Coordinator::TERM_GRACE", 0.3)
      stub_const("RSpec::Core::Parallel::Coordinator::KILL_GRACE", 0.3)
      stub_const("RSpec::Core::Parallel::Coordinator::WATCHDOG_GRACE", 0.1)
      # Inherited by the forked workers: a watchdog that never gets to run.
      allow_any_instance_of(RSpec::Core::Parallel::Worker).to receive(:arm_watchdog)
      write_exiting_suite(0, "if $exiting; trap('TERM') { }; File.write('stuck.pid', Process.pid.to_s); sleep 30; end")
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--order defined --parallel=2 --error-exit-code 3"
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

      # The shutdown timeout, the watchdog grace and both signal graces, with room for a loaded machine.
      expect(elapsed).to be < 0.5 + 0.1 + 0.3 + 0.3 + 5
      expect(last_cmd_exit_status).to eq(3)
      expect(last_cmd_stdout).to include(
        "did not exit within 0.5 seconds of ending the run because the code it ran called `exit` or `abort` (status 0)",
        "still alive 0.3 seconds after SIGTERM; sending SIGKILL"
      )
      stuck = cd('.') { Integer(File.read('stuck.pid')) }
      expect { Process.kill(0, stuck) }.to raise_error(Errno::ESRCH)
    end

    it 'ends the run with that status from a before(:suite) hook too, bounded when the worker then hangs' do
      stub_const("RSpec::Core::Parallel::Coordinator::WATCHDOG_GRACE", 5.0)
      write_file_formatted 'spec/exit_spec.rb', "
        RSpec.configure do |c|
          c.parallel_shutdown_timeout = 0.5
          c.before(:suite) do
            c.after(:suite) do
              $stderr = File.open(\"worker_stderr_\#{Process.pid}.txt\", 'w')
              sleep 30
            end
            exit 4
          end
        end
        RSpec.describe('One') { it('never runs') { File.write('ran', '') } }
        RSpec.describe('Two') { it('never runs') { File.write('ran', '') } }
      "
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--parallel=2 --error-exit-code 3"

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 10
      expect(last_cmd_exit_status).to eq(3)
      expect(last_cmd_stdout).to include("is ending the run: the code it ran called `exit` or `abort` (status 4)")
      dumps = cd('.') { Dir['worker_stderr_*.txt'].map { |file| File.read(file) } }
      expect(dumps).not_to be_empty
      expect(dumps).to all(include("of stopping because the code it ran called `exit` or `abort` (status 4); aborting it."))
      cd('.') { expect(File).not_to exist('ran') }
    end
  end

  describe 'a group tagged `parallel_split: N`' do
    def write_split_suite(extra="")
      write_file_formatted 'spec/split_spec.rb', "
        RSpec.describe 'Split', :parallel_split => 3 do
          before(:context) { File.write(\"context_\#{Process.pid}_\#{rand(1_000_000)}\", '') }
          6.times { |i| it(\"e\#{i}\") { File.write(\"example_\#{i}_\#{Process.pid}\", ''); sleep 0.1 } }
          context 'nested' do
            2.times { |i| it(\"n\#{i}\") { File.write(\"nested_\#{i}_\#{Process.pid}\", '') } }
          end
          #{extra}
        end
        RSpec.describe('Whole') { it('runs once') { } }
      "
    end

    it 'runs it as that many parts in different workers, every example once, with the counts of a serial run' do
      write_split_suite
      run_rspec "--order defined --format json --out serial.json"
      cd('.') { Dir['{context,example,nested}_*'].each { |file| File.delete(file) } }

      run_rspec "--order defined --parallel=3 --format json --out parallel.json --format progress"

      expect(last_cmd_exit_status).to eq(0)
      cd('.') do
        serial, parallel = %w[serial.json parallel.json].map { |file| JSON.parse(File.read(file)) }
        ids = parallel['examples'].map { |example| example['id'] }
        expect(ids.size).to eq(ids.uniq.size)
        expect(ids.sort).to eq(serial['examples'].map { |example| example['id'] }.sort)
        expect(parallel['summary']).to include('example_count' => 9, 'failure_count' => 0)
        expect(Dir['context_*'].size).to eq(3)
        pids = Dir['{example,nested}_*'].map { |file| file[/\d+\z/] }
        expect(pids.uniq.size).to be >= 2
      end
    end

    it 'runs the rest of a part whose worker dies in another worker' do
      write_split_suite("context('crashing') { it('dies') { exit!(7) }; it('after the crash') { File.write('after_crash', '') } }")

      run_rspec "--order defined --parallel=2"

      expect(last_cmd_exit_status).to eq(1)
      expect(last_cmd_stdout).to include("11 examples, 1 failure", "Split crashing dies")
      cd('.') { expect(File).to exist('after_crash') }
    end
  end

  it 'stops its workers, even ones that cannot tear down, when it and its process group get SIGTERM, saying what they ran' do
    write_file_formatted 'spec/wedged_spec.rb', "
      RSpec.configure { |c| c.before(:suite) { File.write(\"worker_\#{RSpec.configuration.parallel_worker_number}.pid\", Process.pid.to_s) } }
      2.times do |i|
        RSpec.describe(\"Wedged \#{i}\") do
          it('keeps a thread that delays exit') do
            File.write(\"running_\#{i}\", '')
            Thread.new { begin; sleep; ensure; sleep 60; end }
            sleep 60
          end
        end
      end
    "
    deadline_for = lambda { |seconds| Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds }
    running = lambda do |pid|
      state = `ps -o stat= -p #{pid}`.strip
      !state.empty? && !state.start_with?('Z')
    end

    cd('.') do
      rspec = [RbConfig.ruby, "-I#{$LOAD_PATH.join(File::PATH_SEPARATOR)}", File.expand_path('../../exe/rspec', __dir__)]
      pid = Process.spawn({ 'TEST_ENV_NUMBER' => nil }, *rspec, '--parallel=2', 'spec/wedged_spec.rb',
                          :out => 'stdout.txt', :err => File::NULL, :pgroup => true)
      deadline = deadline_for.call(20)
      sleep 0.05 until (worker_pids.size == 2 && Dir['running_*'].size == 2) ||
                       Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      workers = worker_pids
      expect(workers.size).to eq(2)

      # What `timeout` does: the command first, then its whole process group.
      Process.kill(:TERM, pid)
      Process.kill(:TERM, -pid)
      _, status = Process.wait2(pid)

      expect(status.termsig).to eq(Signal.list.fetch('TERM'))
      expect(File.read('stdout.txt')).to include("Received SIGTERM; stopping the workers.")
      expect(File.read('stdout.txt').scan(%r{^Parallel worker [12] \(pid \d+\) was running \./spec/wedged_spec\.rb:\d+ \(Wedged [01] keeps a thread that delays exit\) for [\d.]+ seconds?\.$}).size).to eq(2)
      deadline = deadline_for.call(10)
      sleep 0.05 until workers.none?(&running) || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      expect(workers.select(&running)).to eq([])
    end
  end

  it 'leaves SIGHUP ignored when it starts ignoring it, as under nohup, like a serial run' do
    write_file_formatted 'spec/hangup_spec.rb', "
      RSpec.configure { |c| c.before(:suite) { File.write(\"worker_\#{RSpec.configuration.parallel_worker_number}.pid\", Process.pid.to_s) } }
      RSpec.describe('One') { it('hangs up on its own process') { Process.kill('HUP', Process.pid); sleep 0.1 } }
      RSpec.describe('Two') do
        it('outlasts a hangup of the whole run') do
          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
          sleep 0.05 until File.exist?('hung_up') || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        end
      end
    "
    wait_until = lambda do |seconds, &condition|
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
      sleep 0.05 until condition.call || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      condition.call
    end

    cd('.') do
      rspec = [RbConfig.ruby, "-I#{$LOAD_PATH.join(File::PATH_SEPARATOR)}", File.expand_path('../../exe/rspec', __dir__)]
      ignoring_hangups = [RbConfig.ruby, '-e', 'trap("HUP", "IGNORE"); exec(*ARGV)']
      pid = Process.spawn({ 'TEST_ENV_NUMBER' => nil }, *ignoring_hangups, *rspec, '--parallel=2', 'spec/hangup_spec.rb',
                          :out => 'stdout.txt', :err => File::NULL)
      expect(wait_until.call(20) { Dir['worker_*.pid'].size == 2 }).to be true

      Process.kill(:HUP, pid)
      File.write('hung_up', '')
      status = nil
      finished = wait_until.call(30) { status ||= Process.waitpid2(pid, Process::WNOHANG)&.last }
      Process.kill(:KILL, pid) && Process.wait(pid) unless finished

      expect(status&.exitstatus).to eq(0)
      expect(File.read('stdout.txt')).to include("2 examples, 0 failures")
      expect(File.read('stdout.txt')).not_to include("SIGHUP")
    end
  end

  it 'stops workers stuck in an example once the coordinating process is gone' do
    write_file_formatted 'spec/stuck_spec.rb', "
      RSpec.configure { |c| c.before(:suite) { File.write(\"worker_\#{RSpec.configuration.parallel_worker_number}.pid\", Process.pid.to_s) } }
      RSpec.describe('One') { it('sleeps') { sleep 60 } }
      RSpec.describe('Two') { it('sleeps') { sleep 60 } }
    "
    wait_until = lambda do |seconds, &condition|
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
      sleep 0.05 until condition.call || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      condition.call
    end
    # Orphans are adopted by whatever reaps them here -- a container's init may
    # not, leaving zombies, which no longer run.
    running = lambda do |pid|
      state = `ps -o stat= -p #{pid}`.strip
      !state.empty? && !state.start_with?('Z')
    end

    cd('.') do
      rspec = [RbConfig.ruby, "-I#{$LOAD_PATH.join(File::PATH_SEPARATOR)}", File.expand_path('../../exe/rspec', __dir__)]
      Dir.mkdir('tmp')
      pid = Process.spawn({ 'TEST_ENV_NUMBER' => nil, 'TMPDIR' => File.expand_path('tmp') }, *rspec, '--parallel=2',
                          'spec/stuck_spec.rb', :out => File::NULL, :err => 'stderr.txt')
      expect(wait_until.call(20) { worker_pids.size == 2 }).to be true
      workers = worker_pids
      expect(Dir['tmp/rspec-parallel-*'].size).to eq(1)

      Process.kill(:KILL, pid)
      Process.wait(pid)

      expect(wait_until.call(10) { workers.none?(&running) }).to be true
      expect(File.read('stderr.txt').scan("is exiting because the coordinating process (pid #{pid}) is gone").size).to eq(2)
      # Where the workers' output went, which the coordinator would have removed.
      expect(Dir['tmp/*']).to eq([])
    end
  end

  describe 'a worker that finishes but does not exit' do
    def write_hanging_suite(extra="")
      write_file_formatted 'spec/hang_spec.rb', "
        RSpec.configure do |c|
          c.parallel_shutdown_timeout = 0.5
          c.after(:suite) do
            if RSpec.configuration.parallel_worker_number == 2
              $stderr = File.open('worker_stderr.txt', 'w')
              #{extra}
              thread = Thread.new { sleep }
              at_exit { thread.join }
            end
          end
        end
        RSpec.describe('H1') { it('passes') { } }
        RSpec.describe('H2') { it('passes') { } }
      "
    end

    def pids_in(output)
      output.scan(/\(pid (\d+)\)/).flatten.map(&:to_i).uniq
    end

    def alive?(pid)
      Process.kill(0, pid)
      Process.waitpid(pid, Process::WNOHANG).nil?
    rescue Errno::ESRCH, Errno::ECHILD
      false
    end

    it 'is aborted by its own watchdog with a thread dump, within the shutdown timeout, and fails the run' do
      # Leaves the worker's watchdog plenty of room to fire before the
      # coordinator steps in, even on a loaded machine.
      stub_const("RSpec::Core::Parallel::Coordinator::WATCHDOG_GRACE", 5.0)
      write_hanging_suite
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--parallel=2 --error-exit-code 3 spec/hang_spec.rb"
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      worker_stderr = cd('.') { File.read('worker_stderr.txt') }

      expect(last_cmd_exit_status).to eq(3)
      expect(elapsed).to be < 10
      expect(last_cmd_stdout).not_to include("sending SIGTERM")
      expect(last_cmd_stdout).to include("2 examples, 0 failures")
      expect(last_cmd_stdout).to match(/Parallel worker 2 \(pid \d+\) failed while shutting down \(exit status 1\)/)
      expect(worker_stderr).to include("did not exit within 0.5 seconds of finishing its last example group", "Threads at this point:")
      expect(worker_stderr).to match(/[`'](Thread#)?join'/)
      expect(pids_in(last_cmd_stdout)).not_to be_empty
      pids_in(last_cmd_stdout).each { |pid| expect(alive?(pid)).to be false }
    end

    it 'is terminated, then killed, by the coordinator when its own watchdog cannot fire' do
      stub_const("RSpec::Core::Parallel::Coordinator::TERM_GRACE", 0.3)
      stub_const("RSpec::Core::Parallel::Coordinator::KILL_GRACE", 0.3)
      stub_const("RSpec::Core::Parallel::Coordinator::WATCHDOG_GRACE", 0.1)
      # Forked workers inherit this stub, which stands in for a worker whose
      # watchdog thread never gets to run (native code holding the GVL, ...).
      allow_any_instance_of(RSpec::Core::Parallel::Worker).to receive(:arm_watchdog)
      write_hanging_suite("trap('TERM') { }")
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--parallel=2 --error-exit-code 3 spec/hang_spec.rb"
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

      expect(last_cmd_exit_status).to eq(3)
      expect(elapsed).to be < 5
      expect(last_cmd_stdout).to include(
        "did not exit within 0.5 seconds of finishing its last example group",
        "so it was sent SIGTERM",
        "still alive 0.3 seconds after SIGTERM; sending SIGKILL"
      )
      pids_in(last_cmd_stdout).each { |pid| expect(alive?(pid)).to be false }
    end
  end

  # As a worker deadlocked in a fork handler (in Ruby, or a library's) is:
  # it never runs any of RSpec's code, and SIGTERM does not end it.
  describe 'a worker stuck before it runs any of its code' do
    before do
      stub_const("RSpec::Core::Parallel::Coordinator::TERM_GRACE", 0.3)
      stub_const("RSpec::Core::Parallel::Coordinator::KILL_GRACE", 0.3)
      # Forked workers inherit this stub.
      allow_any_instance_of(RSpec::Core::Parallel::Worker).to receive(:run).and_wrap_original do |original, *args|
        if original.receiver.number == 2
          trap('TERM', 'IGNORE')
          sleep
        end
        original.call(*args)
      end
    end

    def write_suite
      write_file_formatted 'spec/stuck_at_fork_spec.rb', "
        RSpec.configure { |c| c.parallel_shutdown_timeout = 0.5 }
        RSpec.describe('S1') { it('passes') { } }
        RSpec.describe('S2') { it('passes') { } }
      "
    end

    def pids_in(output)
      output.scan(/\(pid (\d+)\)/).flatten.map(&:to_i).uniq
    end

    it 'is terminated, then killed, once it misses its start deadline, and its group runs in another worker' do
      write_suite
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec "--parallel=2 --order defined spec/stuck_at_fork_spec.rb"

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 10
      expect(last_cmd_stdout).to include(
        "2 examples, 0 failures",
        "never reported back after it was forked",
        "still alive 0.3 seconds after SIGTERM; sending SIGKILL"
      )
      expect(last_cmd_exit_status).to eq(0)
      pids = pids_in(last_cmd_stdout)
      expect(pids.size).to eq(1)
      expect { Process.kill(0, pids.first) }.to raise_error(Errno::ESRCH)
    end
  end

  it 'falls back to a serial run with a warning inside another parallel worker' do
    write_file_formatted 'spec/a_spec.rb', "
      RSpec.describe('A') { it('passes') { expect(RSpec.configuration.parallel_worker_number).to be_nil } }
      RSpec.describe('B') { it('passes') { } }
    "

    @inside_another_parallel_worker = true
    run_rspec "--parallel=2 spec/a_spec.rb"

    expect(last_cmd_exit_status).to eq(0)
    expect(last_cmd_stdout).to include("2 examples, 0 failures")
    expect(last_cmd_stderr).to include("Ignoring `--parallel`: this process is already a worker of a parallel test run")
  end

  describe 'with --no-parallel-preload' do
    # A worker still loading once every group has been handed out is
    # dismissed, so on a suite this small one worker can take every group
    # before the other has loaded. With `wait_for_both_workers` each example
    # first waits for both workers to have loaded the last file, which keeps
    # groups queued until they have.
    def write_marking_spec_files(wait_for_both_workers: false)
      wait = "deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
              sleep 0.05 until Dir['loaded_m2_*'].size == 3 || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline"
      3.times do |i|
        write_file_formatted "spec/m#{i}_spec.rb", "
          File.write(\"loaded_m#{i}_\#{Process.pid}_\#{ENV.fetch('TEST_ENV_NUMBER', 'none')}\", '')
          RSpec.describe('M#{i}') do
            it('runs in a process that loaded its file') do
              #{wait if wait_for_both_workers}
              expect(File).to exist(\"loaded_m#{i}_\#{Process.pid}_\#{ENV['TEST_ENV_NUMBER']}\")
            end
          end
        "
      end
    end

    def loaders
      cd('.') { Dir['loaded_*'].map { |file| file.split('_', 4).last }.tally }
    end

    it 'has every worker load the spec files itself, once TEST_ENV_NUMBER is set, and run what it loaded' do
      write_marking_spec_files(:wait_for_both_workers => true)

      run_rspec '--parallel=2 --no-parallel-preload'

      expect(last_cmd_stdout).to include("3 examples, 0 failures")
      expect(last_cmd_exit_status).to eq(0)
      expect(loaders).to eq("none" => 3, "" => 3, "2" => 3)
    end

    it 'does not use a worker that loads other examples than the coordinating process' do
      write_marking_spec_files
      # Worker 1 loads a second late, so worker 2 reports what it loaded while
      # there are still groups to hand out, rather than being dismissed unchecked.
      write_file_formatted 'spec/z_spec.rb', "
        sleep 1 if ENV['TEST_ENV_NUMBER'] == ''
        RSpec.describe('Z') { it('passes') { }; it('only in the second worker') { } if ENV['TEST_ENV_NUMBER'] == '2' }
      "

      run_rspec '--parallel=2 --no-parallel-preload'

      expect(last_cmd_stdout).to match(%r{Parallel worker 2 \(pid \d+\) loaded other examples than this process from \./spec/z_spec\.rb \(})
      expect(last_cmd_stdout).to include("4 examples, 0 failures, 1 error occurred outside of examples")
      expect(last_cmd_exit_status).to eq(1)
    end

    it 'terminates a worker still loading long after the coordinating process finished, and runs its groups elsewhere' do
      write_file_formatted 'spec/spec_helper.rb', "RSpec.configure { |c| c.parallel_shutdown_timeout = 1 }"
      # Worker 1 stays busy until worker 2 is gone, however long this
      # process's own loading (and so worker 2's deadline) took.
      3.times do |i|
        write_file_formatted "spec/s#{i}_spec.rb", "
          (File.write('loading.pid.tmp', Process.pid.to_s); File.rename('loading.pid.tmp', 'loading.pid'); sleep 600) if ENV['TEST_ENV_NUMBER'] == '2'
          RSpec.describe('S#{i}') do
            it('keeps the other worker busy past the deadline') do
              $give_up ||= Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
              loading = lambda do
                pid = File.exist?('loading.pid') && Integer(File.read('loading.pid'))
                !pid || Process.kill(0, pid)
              rescue Errno::ESRCH
                false
              end
              sleep 0.05 while loading.call && Process.clock_gettime(Process::CLOCK_MONOTONIC) < $give_up
            end
          end
        "
      end
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      run_rspec '--require ./spec/spec_helper --parallel=2 --no-parallel-preload'

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 20
      expect(last_cmd_stdout).to match(/Parallel worker 2 \(pid \d+\) took longer than [\d.]+ seconds? while loading the spec files, so it was sent SIGTERM\./)
      expect(last_cmd_stdout).to include("3 examples, 0 failures, 1 error occurred outside of examples")
    end

    it 'warns, and runs serially rather than fork after loading, when turned off from a spec file, too late' do
      write_file_formatted 'spec/late_spec.rb', "
        RSpec.configure { |c| c.parallel_preload = false }
        RSpec.describe('Late') { it('runs serially') { expect(RSpec.configuration.parallel_worker_number).to be_nil } }
        RSpec.describe('Later') { it('runs serially') { expect(RSpec.configuration.parallel_worker_number).to be_nil } }
      "

      run_rspec '--parallel=2 spec/late_spec.rb'

      expect(last_cmd_stdout).to include("2 examples, 0 failures")
      expect(last_cmd_stderr).to include("Ignoring `--parallel`: `config.parallel_preload = false` was set while the spec " \
                                         "files were loading", "Running serially.")
    end

    it 'warns, and runs serially rather than fork after loading, when the worker count is set from a spec file, too late' do
      write_file_formatted 'spec/late_spec.rb', "
        RSpec.configure { |c| c.parallel_workers = 2 }
        RSpec.describe('Late') { it('runs serially') { expect(RSpec.configuration.parallel_worker_number).to be_nil } }
        RSpec.describe('Later') { it('runs serially') { expect(RSpec.configuration.parallel_worker_number).to be_nil } }
      "

      run_rspec '--no-parallel-preload spec/late_spec.rb'

      expect(last_cmd_stdout).to include("2 examples, 0 failures")
      expect(last_cmd_stderr).to include("`config.parallel_workers` was set while the spec files were loading", "Running serially.")
    end

    it 'leaves Kernel.rand alone in the coordinating process, never calling srand' do
      write_marking_spec_files
      previous = Kernel.srand(1234)

      run_rspec '--parallel=2 --no-parallel-preload'
      # Drawn first: an `expect` may itself draw, by creating `RSpec.configuration` (rspec/rspec#305 does).
      drawn = Kernel.rand(2**62)

      expect(last_cmd_stdout).to include("3 examples, 0 failures")
      expect(drawn).to eq(Random.new(1234).rand(2**62))
    ensure
      Kernel.srand(previous)
    end

    it 'has every worker draw from a seed of its own, as forked processes do' do
      %w[a b].each do |name|
        write_file_formatted "spec/#{name}_spec.rb", "
          RSpec.describe('#{name.upcase}') do
            it('draws once every worker runs an example') do
              number = RSpec.configuration.parallel_worker_number
              File.write(\"running_\#{number}\", '')
              deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
              sleep 0.05 until Dir['running_*'].size == 2 || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
              File.write(\"drew_\#{number}\", rand(2**62).to_s)
            end
          end
        "
      end

      run_rspec '--parallel=2 --no-parallel-preload'

      expect(last_cmd_stdout).to include("2 examples, 0 failures")
      drawn = cd('.') { Dir['drew_*'].map { |file| File.read(file) } }
      expect(drawn.size).to eq(2)
      expect(drawn.uniq.size).to eq(2)
    end

    it 'reports the groups and examples as the worker that ran them described them' do
      write_file_formatted 'spec/pid_spec.rb', "
        2.times do |i|
          RSpec.describe(\"G\#{i} loaded in \#{Process.pid}\") do
            it(\"ran in \#{Process.pid}\") { File.write(\"ran_\#{Process.pid}\", '') }
          end
        end
      "

      run_rspec '--parallel=2 --no-parallel-preload --format documentation'

      expect(last_cmd_stdout).to include("2 examples, 0 failures")
      described = last_cmd_stdout.scan(/(?:loaded|ran) in (\d+)/).flatten.uniq
      ran = cd('.') { Dir['ran_*'].map { |file| file.delete_prefix('ran_') } }
      expect(described).not_to be_empty
      expect(described).to all(satisfy { |pid| ran.include?(pid) })
      expect(described).not_to include(Process.pid.to_s)
    end

    it 'runs the groups of a single spec file across the workers' do
      write_file_formatted 'spec/one_spec.rb', "
        3.times { |i| RSpec.describe(\"G\#{i}\") { it('runs in a worker') { expect(RSpec.configuration.parallel_worker_number).not_to be_nil } } }
      "

      run_rspec '--parallel=2 --no-parallel-preload spec/one_spec.rb'

      expect(last_cmd_stdout).to include("3 examples, 0 failures")
      expect(last_cmd_stderr).to eq("")
    end

    it 'forks nothing for an --only-failures run that is about to abort for want of a status file' do
      write_marking_spec_files

      # A real process: the abort ends it with `exit!`.
      status, out, err = rspec_process('--parallel=2', '--no-parallel-preload', '--only-failures')

      expect(status.exitstatus).to eq(1)
      expect(err + out).to include("example_status_persistence_file_path")
      expect(loaders).to eq("none" => 3)
    end

    it 'goes on like a serial run when a spec file fails to load in every process' do
      write_marking_spec_files
      write_file_formatted 'spec/broken_spec.rb', "RSpec.describe('Broken') { it('a') { }"

      run_rspec '--parallel=2 --no-parallel-preload'

      expect(last_cmd_stdout).to include("While loading ./spec/broken_spec.rb a `raise SyntaxError` occurred")
      expect(last_cmd_stdout).to include("3 examples, 0 failures, 1 error occurred outside of examples")
      expect(last_cmd_stdout).not_to include("Parallel worker")
    end
  end
end

# Where Ruby has no `fork` (Windows, JRuby, TruffleRuby) the workers are new
# `rspec` processes; elsewhere a `--require`d file takes `fork` away.
RSpec.describe 'Parallel execution without fork (spawned workers)', :slow do
  include_context "aruba support"
  include ParallelRunProcess

  def without_fork
    return [] unless RSpec::Support::RubyFeatures.fork_supported?

    write_file 'no_fork.rb', "module Process\n  class << self\n    undef_method :fork\n  end\nend\n"
    %w[--require ./no_fork]
  end

  # Each group's first example waits for both workers, so that neither runs
  # every group before the other has started.
  def write_suite(helper="")
    write_file 'spec/spec_helper.rb', "
      RSpec.configure do |c|
        c.before(:suite) { File.write(\"worker_\#{c.parallel_worker_number}.txt\", [Process.pid, ENV['TEST_ENV_NUMBER'].inspect, c.seed].join(' ')) }
      end
      def both_workers_started
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
        sleep 0.05 until Dir['worker_*.txt'].size == 2 || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      end
      #{helper}
    "
    3.times do |i|
      write_file "spec/s#{i}_spec.rb", "
        RSpec.describe('S#{i}') do
          it('passes') { both_workers_started; puts 'printed by S#{i}' }
          it('#{i == 1 ? 'fails' : 'passes too'}') { expect(#{i}).not_to eq(1) }
        end
      "
    end
  end

  it 'runs the groups in new rspec processes, with the run’s seed, and reports one run' do
    write_suite
    options = without_fork + %w[--require ./spec/spec_helper --parallel=2 --seed 1234 --format progress
                                --format json --out results.json]

    status, stdout, stderr = rspec_process(*options)

    expect(stdout).to include("6 examples, 1 failure", "Randomized with seed 1234", "printed by S0", "printed by S2")
    expect(stderr).not_to include("Ignoring `--parallel`")
    expect(status.exitstatus).to eq(1)
    results = cd('.') { JSON.parse(File.read('results.json')) }
    expect(results['summary']).to include('example_count' => 6, 'failure_count' => 1)
    workers = cd('.') { Dir['worker_*.txt'].map { |file| File.read(file).split } }
    expect(workers.size).to eq(2)
    expect(workers.map(&:first).uniq.size).to eq(2)
    expect(workers.map { |worker| worker.drop(1) }).to eq([['""', '1234'], ['"2"', '1234']])
  end

  it 'fails the example a spawned worker was running when it is killed, and runs the rest' do
    write_suite "RSpec.describe('Killed') { it('dies') { both_workers_started; Process.kill(:KILL, Process.pid) } }"

    status, stdout, _stderr = rspec_process(*without_fork, '--require', './spec/spec_helper', '--parallel=2')

    expect(stdout).to include("7 examples, 2 failures", "exited unexpectedly", "while running this example")
    expect(status.exitstatus).to eq(1)
  end

  it 'aborts spawned workers that do not exit within the shutdown timeout, and shows their thread dumps' do
    write_suite "RSpec.configure { |c| c.parallel_shutdown_timeout = 1; c.after(:suite) { sleep } }"
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    status, stdout, stderr = rspec_process(*without_fork, '--require', './spec/spec_helper', '--parallel=2')

    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 30
    expect(stdout).to include("6 examples, 1 failure")
    expect(stderr.scan("did not exit within 1 second of finishing its last example group").size).to eq(2)
    expect(status.exitstatus).to eq(1)
  end
end
