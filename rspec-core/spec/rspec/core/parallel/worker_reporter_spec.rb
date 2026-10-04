module RSpec::Core::Parallel
  RSpec.describe WorkerReporter do
    let(:results) { Channel.pipe }
    let(:jobs) { Channel.pipe }
    let(:reporter) { WorkerReporter.new(RSpec.configuration, results.last, jobs.first) }

    def frames
      results.first.drain.map(&:first)
    end

    after do
      results.each(&:close)
      jobs.each(&:close)
    end

    it "forwards group and example notifications as frames with ids and result payloads" do
      group = RSpec.describe("Outer") do
        it("passes") { expect(1).to eq(1) }
        it("fails") { raise ArgumentError, "nope" }
        it("is pending") do
          pending "later"
          raise "x"
        end
        it("is skipped") { skip "not now" }
        context("Inner") { it("nested") {} }
      end
      inner = group.children.first

      group.run(reporter)

      kinds = frames
      expect(kinds.map(&:first)).to eq([
        :group_started,
        :example_started, :example_finished,
        :example_started, :example_finished,
        :example_started, :example_finished,
        :example_started, :example_finished,
        :group_started, :example_started, :example_finished, :group_finished,
        :group_finished
      ])

      expect(kinds[0]).to eq([:group_started, group.id])
      expect(kinds[9]).to eq([:group_started, inner.id])
      expect(kinds[12]).to eq([:group_finished, inner.id, nil])
      expect(kinds[13][0..1]).to eq([:group_finished, group.id])
      expect(kinds[13][2]).to be_a(Float)

      payloads = kinds.select { |f| f.first == :example_finished }.to_h { |f| [f[1], f[2]] }
      passed, failed, pending, skipped = group.examples.map { |example| payloads.fetch(example.id) }

      expect(passed).to include(:status => :passed, :exception => nil, :pending_exception => nil, :pending => nil, :skip => nil)
      expect(passed[:started_at]).to be_a(Time)
      expect(passed[:finished_at]).to be_a(Time)
      expect(passed[:run_time]).to be_a(Float)

      expect(failed[:status]).to eq(:failed)
      expect(failed[:exception]).to include(:class_name => "ArgumentError", :message => "nope")

      expect(pending).to include(:status => :pending, :pending_message => "later", :pending_fixed => false, :pending => true)
      expect(pending[:pending_exception]).to include(:class_name => "RuntimeError", :message => "x")

      expect(skipped).to include(:status => :pending, :pending_message => "not now", :skip => true, :pending_exception => nil)
    end

    it "ships the example ids the coordinator can look up" do
      group = RSpec.describe("G") { it("e") {} }
      group.run(reporter)

      expect(frames.select { |f| f.first == :example_started }).to eq([[:example_started, group.examples.first.id]])
    end

    it "forwards extra failure lines and stringifies result parts that cannot be marshaled" do
      group = RSpec.describe("G") do
        it("fails") do |ex|
          ex.metadata[:extra_failure_lines] = ["more", lambda {}]
          raise "x"
        end
      end
      group.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload[:extra_failure_lines].size).to eq(2)
      expect(payload[:extra_failure_lines].first).to eq("more")
      expect(payload[:extra_failure_lines].last).to match(/Proc/)
    end

    it "forwards messages and deprecations" do
      reporter.message("hello")
      reporter.message(:not_a_string)
      reporter.deprecation(:deprecated => "foo", :message => "msg", :replacement => "bar", :call_site => "here")

      expect(frames).to eq([
        [:message, "hello"],
        [:message, :not_a_string],
        [:deprecation, { :deprecated => "foo", :message => "msg", :replacement => "bar", :call_site => "here" }]
      ])
    end

    it "stringifies messages and deprecation parts that cannot be marshaled" do
      reporter.message(lambda {})
      reporter.deprecation(:deprecated => lambda {}, :message => "msg", :replacement => nil, :call_site => nil)

      message, deprecation = frames
      expect(message.first).to eq(:message)
      expect(message.last).to match(/Proc/)
      expect(deprecation.last[:deprecated]).to match(/Proc/)
      expect(deprecation.last).to include(:message => "msg", :replacement => nil)
    end

    it "forwards custom notifications published through the reporter" do
      reporter.publish(:my_event, :a => 1, :b => "two")
      reporter.publish(:bare_event)

      expect(frames).to eq([[:custom, :my_event, { :a => 1, :b => "two" }], [:custom, :bare_event, {}]])
    end

    it "warns instead of crashing when custom notification options cannot be marshaled" do
      RSpec.configuration.parallel_worker_number = 3
      reporter.publish(:my_event, :callback => lambda {})

      message = frames.first
      expect(message.first).to eq(:message)
      expect(message.last).to include("Parallel worker 3 could not forward the custom `my_event` notification", "Proc")
    end

    it "forwards non-example failures as a frame with the exception payload and the context description" do
      error = RuntimeError.new("suite setup failed")
      error.set_backtrace(["./spec/spec_helper.rb:3"])

      reporter.notify_non_example_exception(error, "An error occurred in a `before(:suite)` hook.")

      kind, payload, description = frames.first
      expect(kind).to eq(:non_example_failure)
      expect(payload).to include(:class_name => "RuntimeError", :message => "suite setup failed", :backtrace => ["./spec/spec_helper.rb:3"])
      expect(description).to eq("An error occurred in a `before(:suite)` hook.")
      expect(RSpec.world.non_example_failure).to be true
    end

    it "ignores the notifications that only the coordinator's reporter emits" do
      reporter.start(1)
      reporter.finish

      expect(frames).to eq([])
    end

    it "does not forward example_passed/failed/pending separately" do
      RSpec.describe("G") { it("e") {} }.run(reporter)

      expect(frames.map(&:first)).not_to include(:example_passed)
    end

    it "sets the world's `wants_to_quit` when the coordinator asks it to quit between examples" do
      jobs.last.send([:quit])
      group = RSpec.describe("G") do
        it("first") {}
        it("second") {}
      end

      group.run(reporter)

      expect(RSpec.world.wants_to_quit).to be true
      expect(frames.count { |f| f.first == :example_finished }).to eq(1)
    end

    it "leaves other control messages for the worker's job loop while polling" do
      jobs.last.send([:run, 7])
      RSpec.describe("G") { it("e") {} }.run(reporter)

      expect(RSpec.world.wants_to_quit).to be_falsey
      expect(jobs.first.receive).to eq([:run, 7])
    end

    it "keeps the reporter's own failure bookkeeping so fail-fast works inside the worker" do
      RSpec.configuration.fail_fast = true
      group = RSpec.describe("G") do
        it("fails") { raise "x" }
        it("never runs") {}
      end

      group.run(reporter)

      expect(reporter.failed_examples.size).to eq(1)
      expect(RSpec.world.wants_to_quit).to be true
    end
  end
end
