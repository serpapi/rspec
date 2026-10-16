# RSpec::Core

This is the detailed readme for `rspec-core`, see also:

* [The combined readme](../README.md)
* [rspec-expectations](../rspec-expectations/README.md)
* [rspec-mocks](../rspec-mocks/README.md)
* [rspec-support](../rspec-support/README.md)

## Basic Structure

RSpec uses the words "describe" and "it" so we can express concepts like a conversation:

    "Describe an order."
    "It sums the prices of its line items."

```ruby
RSpec.describe Order do
  it "sums the prices of its line items" do
    order = Order.new

    order.add_entry(LineItem.new(:item => Item.new(
      :price => Money.new(1.11, :USD)
    )))
    order.add_entry(LineItem.new(:item => Item.new(
      :price => Money.new(2.22, :USD),
      :quantity => 2
    )))

    expect(order.total).to eq(Money.new(5.55, :USD))
  end
end
```

The `describe` method creates an [ExampleGroup](http://rubydoc.info/gems/rspec-core/RSpec/Core/ExampleGroup).  Within the
block passed to `describe` you can declare examples using the `it` method.

Under the hood, an example group is a class in which the block passed to
`describe` is evaluated. The blocks passed to `it` are evaluated in the
context of an _instance_ of that class.

## Nested Groups

You can also declare nested groups using the `describe` or `context`
methods:

```ruby
RSpec.describe Order do
  context "with no items" do
    it "behaves one way" do
      # ...
    end
  end

  context "with one item" do
    it "behaves another way" do
      # ...
    end
  end
end
```

Nested groups are subclasses of the outer example group class, providing
the inheritance semantics you'd want for free.

## Aliases

You can declare example groups using either `describe` or `context`.
For a top-level example group, `describe` and `context` are available
off of `RSpec`.

You can declare examples within a group using any of `it`, `specify`, or
`example`.

## Shared Examples and Contexts

Declare a shared example group using `shared_examples`, and then include it
in any group using `it_behaves_like` or `include_examples`.

```ruby
RSpec.shared_examples "collections" do |collection_class|
  it "is empty when first created" do
    expect(collection_class.new).to be_empty
  end
end

RSpec.shared_examples "implements #empty?" do
  it "returns true when there are no members" do
    expect(empty_target).to be_empty
  end

  it "returns false when there are members" do
    expect(non_empty_target).to_not be_empty
  end
end

RSpec.describe Array do
  include_examples "collections", Array

  it_behaves_like "implements #empty?" do
    let(:empty_target) { Array.new }
    let(:non_empty_target) { [:key] }
  end
end

RSpec.describe Hash do
  include_examples "collections", Hash

  it_behaves_like "implements #empty?" do
    let(:empty_target) { Hash.new }
    let(:non_empty_target) { {key: :value} }
  end
end
```

Note that `include_examples` directly includes examples into the current context, whilst
`it_behaves_like` wraps the examples in a new context which allows you to pass a block
to customise things like `let` or `subject` without using local variables passed in.

Nearly anything that can be declared within an example group can be declared
within a shared example group. This includes `before`, `after`, and `around`
hooks, `let` declarations, and nested groups/contexts.

You can also use the names `shared_context` and `include_context`. These are
pretty much the same as `shared_examples` and `include_examples`, providing
more accurate naming when you share hooks, `let` declarations, helper methods,
etc, but no examples.

If you want to reuse shared examples or contexts across your RSpec suite you can
define them in a stand alone _*.rb_ files (_spec/support/shared_examples/definition.rb_
for example). But you will have to manually `require` them (there is no autoloading of
_spec/support/_ directory unless you set it up yourself).

## Metadata

rspec-core stores a metadata hash with every example and group, which
contains their descriptions, the locations at which they were
declared, etc, etc. This hash powers many of rspec-core's features,
including output formatters (which access descriptions and locations),
and filtering before and after hooks.

Although you probably won't ever need this unless you are writing an
extension, you can access it from an example like this:

```ruby
it "does something" do |example|
  expect(example.metadata[:description]).to eq("does something")
end
```

### `described_class`

When a class is passed to `describe`, you can access it from an example
using the `described_class` method, which is a wrapper for
`example.metadata[:described_class]`.

```ruby
RSpec.describe Widget do
  example do
    expect(described_class).to equal(Widget)
  end
end
```

This is useful in extensions or shared example groups in which the specific
class is unknown. Taking the collections shared example group from above, we can
clean it up a bit using `described_class`:

```ruby
RSpec.shared_examples "collections" do
  it "is empty when first created" do
    expect(described_class.new).to be_empty
  end
end

RSpec.describe Array do
  include_examples "collections"
end

RSpec.describe Hash do
  include_examples "collections"
end
```

## A Word on Scope

RSpec has two scopes:

* **Example Group**: Example groups are defined by a `describe` or
  `context` block, which is eagerly evaluated when the spec file is
  loaded. The block is evaluated in the context of a subclass of
  `RSpec::Core::ExampleGroup`, or a subclass of the parent example group
  when you're nesting them.
* **Example**: Examples -- typically defined by an `it` block -- and any other
  blocks with per-example semantics -- such as a `before(:example)` hook -- are
  evaluated in the context of
  an _instance_ of the example group class to which the example belongs.
  Examples are _not_ executed when the spec file is loaded; instead,
  RSpec waits to run any examples until all spec files have been loaded,
  at which point it can apply filtering, randomization, etc.

To make this more concrete, consider this code snippet:

``` ruby
RSpec.describe "Using an array as a stack" do
  def build_stack
    []
  end

  before(:example) do
    @stack = build_stack
  end

  it 'is initially empty' do
    expect(@stack).to be_empty
  end

  context "after an item has been pushed" do
    before(:example) do
      @stack.push :item
    end

    it 'allows the pushed item to be popped' do
      expect(@stack.pop).to eq(:item)
    end
  end
end
```

Under the covers, this is (roughly) equivalent to:

``` ruby
class UsingAnArrayAsAStack < RSpec::Core::ExampleGroup
  def build_stack
    []
  end

  def before_example_1
    @stack = build_stack
  end

  def it_is_initially_empty
    expect(@stack).to be_empty
  end

  class AfterAnItemHasBeenPushed < self
    def before_example_2
      @stack.push :item
    end

    def it_allows_the_pushed_item_to_be_popped
      expect(@stack.pop).to eq(:item)
    end
  end
end
```

To run these examples, RSpec would (roughly) do the following:

``` ruby
example_1 = UsingAnArrayAsAStack.new
example_1.before_example_1
example_1.it_is_initially_empty

example_2 = UsingAnArrayAsAStack::AfterAnItemHasBeenPushed.new
example_2.before_example_1
example_2.before_example_2
example_2.it_allows_the_pushed_item_to_be_popped
```

## The `rspec` Command

When you install the rspec-core gem, it installs the `rspec` executable,
which you'll use to run rspec. The `rspec` command comes with many useful
options.
Run `rspec --help` to see the complete list.

## Running Examples in Parallel

`rspec --parallel` runs the top-level example groups of your suite across
worker processes, one per processor by default (`--parallel=4` for four).
RSpec loads the suite once, forks the workers from the process that loaded it,
hands out groups to whichever worker is free -- longest first when a previous
run recorded timings in the `example_status_persistence_file_path` file -- and
streams results back as examples finish. Your formatters run only in the
coordinating process and see the notifications of a normal run, so there is
one seed, one summary, one profile and one exit code, and `--fail-fast`,
`--only-failures`, `--next-failure`, `--seed`, filters and `path:line`
selection keep working. Once the `--fail-fast` limit is reached no further
groups are handed out and busy workers stop after their current example, so
examples already running in other workers can still add failures. You can make
it the default with:

```ruby
RSpec.configure do |config|
  config.parallel_workers = :auto # or an Integer
end
```

Each worker runs the `before(:suite)` and `after(:suite)` hooks once. Inside a
worker, `RSpec.configuration.parallel_worker_number` is its 1-based number and
`ENV['TEST_ENV_NUMBER']` follows the convention of parallel_tests (`""` for the
first worker, then `"2"`, `"3"`, ...), so anything you already namespace per
worker for that tool keeps working. Each worker draws from `Kernel.rand` with
a seed of its own, as forked processes do, so `Kernel.srand config.seed` does
not make what examples draw reproducible under `--parallel`.

Workers inherit everything that happened while the suite was loaded. For a
Rails application that means `rails_helper` has already evaluated
`database.yml` and connected to the database in the coordinating process
before any worker exists: setting `TEST_ENV_NUMBER` afterwards does **not**
give workers separate databases, so such Rails suites run under `--parallel`
share one test database until rspec-rails adds per-worker database support.
Anything the spec files open while they load -- connections, servers,
threads -- is inherited by every worker; acquire such resources in
`before(:suite)` to get one per worker. A database client that connected while
the spec files loaded (a query at load time is enough) hands the same
connection to every worker unless it reconnects after `fork` by itself: as with
app servers that preload, make each worker reconnect in a `before(:suite)`
hook (with Mongoid, `Mongoid.disconnect_clients`), or have the workers load the
spec files themselves (below). On macOS, the system networking libraries do not
survive `fork` well either: when loading the spec files does a lot of work
(resolving host names, say), workers can crash in `getaddrinfo` or hang.

Or have every worker load the spec files itself: with `--no-parallel-preload`
(or `config.parallel_preload = false` in a `--require`d file) the workers are
forked as soon as the `--require`d files are loaded, before the spec files.
This and the worker count must be known by then: set in a helper that the spec
files require, they come too late, and the run is serial, with a warning --
forking after loading is what this setting avoids. There are no more workers
than spec files (but two for a single file). Each worker then loads the spec
files with its `TEST_ENV_NUMBER` already set, and the coordinating process
loads them too, for its reporting. RSpec never calls `srand`, so each worker
draws from `Kernel.rand` with a seed of its own while loading too, whatever
the coordinating process seeded it with. Nothing the spec files do while
loading is shared then -- connections, threads, system libraries they
initialize -- and helpers they require (rather than `.rspec`) load in every
worker, so a Rails app whose spec files `require 'rails_helper'` boots in each
worker with its own `TEST_ENV_NUMBER`. The cost is loading the spec files once
per process: more CPU and memory, though they load at the same time, and
whatever they do at load time is done that many times. A worker is given work
only once it has loaded the examples the coordinating process has (the same
ids and locations), and what it runs is reported as it described it, so a
description computed while loading -- from the time or `rand`, say -- is the
worker's. One that loads other examples, gets a load error the coordinating
process did not get, or is still loading long after the coordinating process
finished is not used: that counts as an error outside of examples, and its
groups run in the other workers -- or, when no worker is left, do not run.

Formatters receive examples with their status, timing, pending details,
exceptions (class name, message, backtrace, cause chain and aggregated
failures; rebuilt as a subclass of the original class when that class exists
in the coordinating process) and the group and example notifications in the
order the worker produced them, plus `reporter.message`, deprecations and
custom notifications whose options can be marshaled. What an example, its
hooks or a worker listener do to the example's metadata -- keys set, changed in
place or deleted, and the description the matcher of a one-liner like
`it { is_expected.to eq(5) }` generates -- reaches the coordinating process
before formatters see the example. RSpec's own keys stay the coordinating
process's; a value that cannot be marshaled is named in a warning, once per
key, and formatters see the value it had before the example. Changes to a
group's metadata are not forwarded.

What workers print -- `puts` in an example, warnings, the output of processes
they start, a crash report, `at_exit` handlers -- appears on the coordinating
process's standard output and error with the example group that printed it,
as in a serial run: workers write it to files in a private temporary
directory, which the coordinating process reads and then removes. So a
worker's standard output and error are not terminals (`$stdout.tty?` is
false), what an example prints appears once the worker next reports (when the
example finishes, at the latest), and an interactive debugger (`binding.irb`,
`debug`) needs a serial run. With `--no-parallel-preload`, what workers print
while they load the spec files is left out: the coordinating process printed
the same when it loaded them. Listeners registered from
inside a worker (for example with `reporter.register_listener` in a
`before(:suite)` hook) are notified of that worker's example and group
notifications before they are forwarded; run-level notifications (`start`,
`dump_summary`, `close`, ...) only reach the coordinating process.

A worker that has finished its last example group but does not exit (a
non-daemon thread, a browser that will not quit, ...) is given
`config.parallel_shutdown_timeout` seconds (30 by default) to run its
`after(:suite)` hooks and `at_exit` handlers; after that it dumps the
backtraces of all its threads to stderr and is aborted, and the run exits
with `config.error_exit_code`. The coordinator never waits unboundedly for a
finished worker -- not even one whose watchdog cannot run and that ignores
SIGTERM: the run waits for it about the shutdown timeout plus 11 seconds at
most (a second of grace, then 5 seconds after SIGTERM and 5 after SIGKILL).
A worker that never gets past `fork` (deadlocked in a fork handler, in Ruby or
in a library) is terminated the same way once it has not reported back within
the shutdown timeout of being forked, and the group it was given runs in
another worker.

A worker that crashes or is killed (`exit!`, a segfault, the OOM killer, ...)
fails the example it was running with the reason, so the example is listed
under "Failed examples" and picked up by `--only-failures`, and is not run
again -- whatever it did may be why the worker died; the examples of its group
it had not started yet run in another worker. One lost in a `before(:context)`
or `after(:context)` hook fails the examples of that group it had not started,
as such a hook raising an error would, and the rest of its group runs
elsewhere. While groups wait to run, a lost worker is replaced by a new one
with its number and `TEST_ENV_NUMBER` -- not once the run stops early (fail-fast,
an interrupt, `exit`), not for a worker lost while loading the spec files, and
no more often than the run has workers; under `--no-parallel-preload` with
`fork` the new one is a new `rspec` process, as without `fork`, since forking
the coordinating process after it loaded the spec files is what that setting
avoids. Workers that run out of work wait for the busy ones, so that there is
a worker to run what a lost one leaves; if none remains, the run says which
examples did not run and how to run them. Problems between example groups count
as errors outside of examples.

RSpec does not rescue `SystemExit`, so in a serial run code that calls `exit`
(or `abort`) ends the run with that status. Under `--parallel` it ends the run
the same way: no further examples start, busy workers stop after the example
they are running, the summary covers what ran and the run exits with the status
passed to `exit` -- unless a worker then had to be aborted or killed for not
exiting in time. Which examples ran before that depends on timing. To make such
an `exit` fail the example instead, rescue `SystemExit` in an `around` hook.

Workers also exit on their own if the
coordinating process disappears. One that cannot -- stopped, or stuck before
it runs any of RSpec's code, as a deadlock in a `fork` handler leaves it -- is
stopped by a small process the run starts before forking its first worker,
which outlives a coordinating process killed with SIGKILL: it gives the
workers it was told about a moment to leave, then sends them SIGTERM and 5
seconds later SIGKILL, and removes their output files. It signals only those
workers, and only while they are in the run's process group; one forked
moments before the coordinating process was killed may not have been
registered yet. SIGTERM or SIGHUP to the coordinating
process, as `timeout` or a cancelled CI job sends, stops every worker -- even
one that cannot tear down -- before the process ends the way a serial run
would, and the output says which example each worker was running and for how
long, which is usually why the run had to be stopped; a signal the run was
started ignoring (`nohup`), or that your suite installs a handler for, is left
alone. Time the whole run spends stopped (Ctrl-Z) or the machine asleep does
not count towards these timeouts.

A run is never shorter than its longest top-level example group, which a
single worker runs. `--profile` shows the slowest groups; tag one whose
examples can run at the same time in different processes with the number of
parts to run it as, and workers take the parts like any other group:

```ruby
RSpec.describe Pricing, :parallel_split => 4 do
  # ...
end
```

Each part is a slice of the order the group's examples run in, of about the
same total run time when a previous run recorded run times in the
`example_status_persistence_file_path` file (of the same number of examples
otherwise), and runs as `rspec spec/pricing_spec.rb[1:2,1:3]` would:
`before(:context)` and `after(:context)` hooks run once per part, so a group
that sets up something expensive or shared in them may be better left whole.
There are no more parts than examples, the number of parts does not depend on
the machine, and a group whose examples (or those of a group in it) use a
custom ordering is not split, with a warning. With a formatter that follows
example groups (documentation, HTML), each part reads as a block of its own,
under the group's description; `--profile` adds up the parts.

Where Ruby has no `fork` (Windows, JRuby, TruffleRuby), the workers are new
processes running the `rspec` command the run was started with -- the same
arguments, `.rspec` files and `SPEC_OPTS`, without the formatters, and the
run's seed -- and each loads the spec files itself, as with
`--no-parallel-preload`. Their jobs and results travel over their standard
input and output, so what they print goes through the coordinating process
as described above. Configuration done in-process before
`RSpec::Core::Runner.run` cannot reach such workers, so there a run that was
not started by the `rspec` command runs serially, with a warning. Each worker
starts its own Ruby (seconds on JRuby), and on Windows workers that have to
be stopped get SIGKILL, which is all Windows can send. When `TEST_ENV_NUMBER`
is already set because another tool is running RSpec in parallel, RSpec
prints a warning and runs serially. When the system refuses a worker -- too many open
files or processes -- the run goes on with the workers it got, and serially if
it got none, with a warning. `--bisect` and `--dry-run` always run serially,
and the reproduction command `--bisect` prints includes `--no-parallel`, which
(like `--parallel=0`) overrides `--parallel` in `.rspec` and
`config.parallel_workers` in a helper, and leaves `--parallel` out of its
`SPEC_OPTS`.

Each worker is a separate process, so a coverage tool records only the
coordinating process unless it merges forked processes. With SimpleCov, set
`merge_subprocesses true` in the `SimpleCov.start` block (SimpleCov 1.3; it
was `enable_for_subprocesses` before): rspec-core's own suite then measures
the same coverage with `--parallel=8`, preloaded or not, as it does serially.
Workers started as new processes (no `fork`) start SimpleCov from your spec
helper themselves, and SimpleCov 1.3.2 merges their results.

## Store Command Line Options `.rspec`

You can store command line options in a `.rspec` file in the project's root
directory, and the `rspec` command will read them as though you typed them on
the command line.

## Get Started

Start with a simple example of behavior you expect from your system. Do
this before you write any implementation code:

```ruby
# in spec/calculator_spec.rb
RSpec.describe Calculator do
  describe '#add' do
    it 'returns the sum of its arguments' do
      expect(Calculator.new.add(1, 2)).to eq(3)
    end
  end
end
```

Run this with the rspec command, and watch it fail:

```
$ rspec spec/calculator_spec.rb
./spec/calculator_spec.rb:1: uninitialized constant Calculator
```

Address the failure by defining a skeleton of the `Calculator` class:

```ruby
# in lib/calculator.rb
class Calculator
  def add(a, b)
  end
end
```

Be sure to require the implementation file in the spec:

```ruby
# in spec/calculator_spec.rb
# - RSpec adds ./lib to the $LOAD_PATH
require "calculator"
```

Now run the spec again, and watch the expectation fail:

```
$ rspec spec/calculator_spec.rb
F

Failures:

  1) Calculator#add returns the sum of its arguments
     Failure/Error: expect(Calculator.new.add(1, 2)).to eq(3)

       expected: 3
            got: nil

       (compared using ==)
     # ./spec/calculator_spec.rb:6:in `block (3 levels) in <top (required)>'

Finished in 0.00131 seconds (files took 0.10968 seconds to load)
1 example, 1 failure

Failed examples:

rspec ./spec/calculator_spec.rb:5 # Calculator#add returns the sum of its arguments
```

Implement the simplest solution, by changing the definition of `Calculator#add` to:

```ruby
def add(a, b)
  a + b
end
```

Now run the spec again, and watch it pass:

```
$ rspec spec/calculator_spec.rb
.

Finished in 0.000315 seconds
1 example, 0 failures
```

Use the `documentation` formatter to see the resulting spec:

```
$ rspec spec/calculator_spec.rb --format doc
Calculator
  #add
    returns the sum of its arguments

Finished in 0.000379 seconds
1 example, 0 failures
```
