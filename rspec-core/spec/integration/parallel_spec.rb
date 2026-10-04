require 'support/aruba_support'
require 'json'

RSpec.describe 'Parallel execution (--parallel)', :slow, :skip => !RSpec::Support::RubyFeatures.fork_supported? do
  include_context "aruba support"

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
    expect(last_cmd_stdout).to include("11 examples, 4 failures, 2 pending")
    expect(last_cmd_stdout).to include("Alpha fails", "Alpha nested fails with a cause", "Beta aggregates", "Beta fails in an after hook")
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
        expect(parallel).to include("11 examples, 4 failures, 2 pending")
      end
    end

    it 'merges the profile with the durations measured in the workers' do
      run_rspec "--order defined --profile 2 --parallel=2"

      expect(last_cmd_stdout).to include("Top 2 slowest examples", "Top 2 slowest example groups")
      expect(last_cmd_stdout).to match(/\d+\.\d+ seconds average \(\d+\.\d+ seconds \/ \d+ examples?\) \.\/spec\/\w+_spec\.rb:1/)
    end
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
          @output.puts \"\#{'  ' * @depth}ok \#{notification.example.metadata[:flag].inspect}\"
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
      "  ok 7",
      "  FAILED MyAppError app_error=true std=true msg=app broke",
      "Failure/Error: it('raises an app error') { raise MyAppError, 'app broke' }",
      "< One",
      "> Two (feature)",
      "  ok nil",
      "< Two",
      "custom: {from: \"worker\"}",
      "message: Parallel worker 1 could not forward the custom `my_event` notification because its options cannot be marshaled: {payload: #<Proc"
    )
    expect(last_cmd_exit_status).to eq(1)
  end

  it 'honours --fail-fast across workers by asking them to stop between examples' do
    write_file_formatted 'spec/fast_fail_spec.rb', "
      RSpec.describe 'Fails first' do
        it('fails') { expect(1).to eq(2) }
      end
    "
    write_file_formatted 'spec/slow_spec.rb', "
      RSpec.describe 'Slow' do
        10.times { |i| it(\"example \#{i}\") { sleep 0.15 } }
      end
    "

    run_rspec "--order defined --fail-fast --parallel=2"

    expect(last_cmd_exit_status).to eq(1)
    expect(last_cmd_stdout).to match(/\b[1-4] examples?, 1 failure/)
  end

  it 'persists example statuses from every worker and reruns only the failures with --only-failures' do
    write_fixed_suite

    RSpec.configuration.example_status_persistence_file_path = 'statuses.txt'
    run_rspec "--order defined --parallel=2"
    expect(last_cmd_stdout).to include("11 examples, 4 failures, 2 pending")
    cd '.' do
      statuses = File.read("statuses.txt")
      expect(statuses.scan('| passed ').size).to eq(5)
      expect(statuses.scan('| failed ').size).to eq(4)
      expect(statuses.scan('| pending').size).to eq(2)
      expect(statuses).to match(/\d+\.\d+ seconds/)
    end

    RSpec.configuration.example_status_persistence_file_path = 'statuses.txt'
    run_rspec "--order defined --parallel=2 --only-failures"
    expect(last_cmd_stdout).to include("4 examples, 4 failures")
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

  it 'reports a worker that dies and fails the run with the error exit code' do
    write_file_formatted 'spec/crash_spec.rb', "
      RSpec.describe 'Crashing' do
        it('finishes') { }
        it('dies') { exit!(7) }
        it('never runs') { }
      end
      RSpec.describe('Fine') { it('passes') { } }
    "

    run_rspec "--order defined --parallel=2 --error-exit-code 3 spec/crash_spec.rb"

    expect(last_cmd_exit_status).to eq(3)
    expect(last_cmd_stdout).to match(/Parallel worker 1 \(pid \d+\) exited unexpectedly \(exit status 7\) while running Crashing/)
    expect(last_cmd_stdout).to include("0 failures")
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
      expect(worker_stderr).to include("did not exit within 0.5 seconds of finishing its last example group", "Threads at this point:", "Thread#join")
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
        "sending SIGTERM",
        "still alive 0.3 seconds after SIGTERM; sending SIGKILL"
      )
      pids_in(last_cmd_stdout).each { |pid| expect(alive?(pid)).to be false }
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
end
