Feature: `--parallel` option

  Use the `--parallel[=COUNT]` option to run the top-level example groups of
  your suite across several worker processes. RSpec loads the suite once, forks
  the workers from the process that loaded it, and hands groups out to whichever
  worker is free. Results stream back as examples finish, so you get a single
  report with one seed, one summary, one profile and one exit code -- produced
  by the same formatters that a serial run uses.

  Without a count, one worker per processor is used. Set
  `config.parallel_workers` in `RSpec.configure` to make it the default.

  Each worker runs the `before(:suite)` and `after(:suite)` hooks once. In a
  worker, `RSpec.configuration.parallel_worker_number` is its 1-based number
  and `ENV['TEST_ENV_NUMBER']` follows the convention of parallel_tests (`""`
  for the first worker, then `"2"`, `"3"`, ...), so resources that are already
  namespaced per worker for that tool keep working. Everything that happens
  while the suite is loaded -- such as a Rails `rails_helper` evaluating
  `database.yml` and connecting -- happens once, in the coordinating process,
  and is inherited by every worker.

  With `--no-parallel-preload` the workers are forked before the spec files
  are loaded instead, and each loads them itself with its `TEST_ENV_NUMBER`
  already set, so nothing the spec files do while loading is shared. A worker
  is given work only once it has loaded the same examples as the coordinating
  process.

  A worker that crashes fails the example it was running, and the examples of
  its group it had not started run in another worker. Code that calls `exit`
  ends the run with that status, as it does in a serial run.

  `--parallel` needs `fork`. Where it is not available (Windows, JRuby), or
  when `TEST_ENV_NUMBER` is already set because another tool is running RSpec
  in parallel, RSpec prints a warning and runs serially.

  Background:
    Given a file named "spec/alpha_spec.rb" with:
      """ruby
      RSpec.configure do |config|
        config.before(:suite) do
          File.write("worker_#{config.parallel_worker_number}", ENV['TEST_ENV_NUMBER'].inspect)
        end
      end

      RSpec.describe "Alpha" do
        it "passes" do
          expect(1).to eq(1)
        end

        it "fails" do
          expect(1).to eq(2)
        end
      end
      """
    And a file named "spec/beta_spec.rb" with:
      """ruby
      RSpec.describe "Beta" do
        it "passes" do
          expect(2).to eq(2)
        end

        context "nested" do
          it "passes too" do
            expect(3).to eq(3)
          end
        end
      end
      """

  Scenario: Running a suite across two workers produces one ordinary report
    When I run `rspec --parallel=2 --order defined`
    Then the exit status should be 1
    And the output should contain "4 examples, 1 failure"
    And the output should contain "rspec ./spec/alpha_spec.rb:12 # Alpha fails"
    And the output should contain "expected: 2"
    And the file "worker_1" should contain exactly:
      """
      ""
      """
    And the file "worker_2" should contain exactly:
      """
      "2"
      """

  Scenario: Formatters that follow group boundaries see each group whole
    When I run `rspec --parallel=2 --order defined --format documentation`
    Then the exit status should be 1
    And the output should contain:
      """
      Beta
        passes
        nested
          passes too
      """
    And the output should contain:
      """
      Alpha
        passes
        fails (FAILED - 1)
      """

  Scenario: A worker that finishes but never exits cannot hang the run
    Given a file named "spec/hang_spec.rb" with:
      """ruby
      RSpec.configure do |config|
        config.parallel_shutdown_timeout = 1

        config.after(:suite) do
          if config.parallel_worker_number == 2
            thread = Thread.new { sleep }
            at_exit { thread.join }
          end
        end
      end

      RSpec.describe "Gamma" do
        it "passes" do
          expect(4).to eq(4)
        end
      end
      """
    When I run `rspec --parallel=2 --error-exit-code 3 spec/beta_spec.rb spec/hang_spec.rb`
    Then the exit status should be 3
    And the output should contain "3 examples, 0 failures"
    And the output should match /Parallel worker 2 \(pid \d+\) failed while shutting down \(exit status 1\)/
    And the stderr should contain "did not exit within 1 second of finishing its last example group"
    And the stderr should contain "Threads at this point:"
    And the stderr should contain "join"

  Scenario: A worker that crashes leaves the rest of its group to another worker
    Given a file named "spec/crash_spec.rb" with:
      """ruby
      RSpec.describe "Crashing" do
        it "dies" do
          exit!(7)
        end

        it "runs in another worker" do
          expect(5).to eq(5)
        end
      end
      """
    When I run `rspec --parallel=2 --order defined`
    Then the exit status should be 1
    And the output should contain "6 examples, 2 failures"
    And the output should contain "exited unexpectedly (exit status 7) while running this example; 1 example of Crashing"
    And the output should contain "rspec ./spec/crash_spec.rb:2 # Crashing dies"

  Scenario: Code that calls `exit` ends the run with that status, as in a serial run
    Given a file named "spec/exit_spec.rb" with:
      """ruby
      RSpec.describe "Exiting" do
        it "exits" do
          exit 3
        end
      end
      """
    When I run `rspec --parallel=2 --order defined`
    Then the exit status should be 3
    And the output should contain "is ending the run: the code it ran called `exit` or `abort` (status 3)"

  Scenario: With `--no-parallel-preload` workers run the groups they loaded themselves
    Given a file named "spec/gamma_spec.rb" with:
      """ruby
      File.write("loaded_by_#{ENV.fetch('TEST_ENV_NUMBER', 'the coordinating process')}", "")

      RSpec.describe "Gamma" do
        it "runs in a process that loaded this file" do
          expect(File).to exist("loaded_by_#{ENV['TEST_ENV_NUMBER']}")
        end
      end
      """
    When I run `rspec --parallel=2 --no-parallel-preload --order defined`
    Then the exit status should be 1
    And the output should match /^5 examples, 1 failure$/
    And the file "loaded_by_the coordinating process" should exist

  Scenario: Inside another tool's parallel worker RSpec runs serially
    Given I set the environment variable "TEST_ENV_NUMBER" to "2"
    When I run `rspec --parallel=2 --order defined spec/beta_spec.rb`
    Then the exit status should be 0
    And the output should contain "2 examples, 0 failures"
    And the stderr should contain "Ignoring `--parallel`: this process is already a worker of a parallel test run"
