require 'rspec/core/parallel/coordinator'

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

      expect(kinds[0]).to eq([:group_started, group.id, { :description => "Outer", :full_description => "Outer" }])
      expect(kinds[9]).to eq([:group_started, inner.id, { :description => "Inner", :full_description => "Outer Inner" }])
      expect(kinds[12]).to eq([:group_finished, inner.id, nil])
      expect(kinds[13][0..1]).to eq([:group_finished, group.id])
      expect(kinds[13][2]).to be_a(Float)

      payloads = kinds.select { |f| f.first == :example_finished }.to_h { |f| [f[1], f[2]] }
      passed, failed, pending, skipped = group.examples.map { |example| payloads.fetch(example.id) }

      expect(passed).to include(:status => :passed, :exception => nil, :pending_exception => nil, :pending => nil, :skip => nil,
                                :description => "passes", :full_description => "Outer passes")
      expect(passed[:started_at]).to eq([group.examples.first.execution_result.started_at.tv_sec,
                                         group.examples.first.execution_result.started_at.tv_nsec])
      expect(passed[:finished_at]).to all(be_an(Integer))
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

    it "ships the description a one-liner's matcher generated while it ran" do
      group = RSpec.describe("G") { it { expect(1).to eq(1) } }
      group.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload).to include(:description => "is expected to eq 1", :full_description => "G is expected to eq 1")
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

    it "notifies listeners registered inside the worker, such as from a `before(:suite)` hook" do
      events = []
      notifications = [:example_group_started, :example_started, :example_finished, :example_failed, :message, :my_event]
      listener = Object.new
      notifications.each do |event|
        listener.define_singleton_method(event) { |_notification| events << event }
      end
      reporter.register_listener(listener, *notifications)

      RSpec.describe("G") { it("fails") { raise "x" } }.run(reporter)
      reporter.message("hi")
      reporter.publish(:my_event)

      expect(events).to eq(notifications)
      expect(frames.map(&:first)).to eq([:group_started, :example_started, :example_finished, :group_finished, :message, :custom])
    end

    it "forwards metadata that a worker listener adds when notified of the example's outcome" do
      listener = Object.new
      def listener.example_failed(notification)
        notification.example.metadata[:extra_failure_lines] = ["Ref: added in the worker"]
      end
      reporter.register_listener(listener, :example_failed)

      RSpec.describe("G") { it("fails") { raise "x" } }.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload[:extra_failure_lines]).to eq(["Ref: added in the worker"])
    end

    it "forwards the metadata an example set, changed in place or deleted, and nothing for one that changed none" do
      group = RSpec.describe("G", :kept => "k", :gone => true) do
        it("changes", :tags => ["a"], :same => "s") do |ex|
          ex.metadata[:added] = { :path => "shot.png" }
          ex.metadata[:tags] << "b"
          ex.metadata.store(:same, ex.metadata[:same])
          ex.metadata.delete(:gone)
        end
        it("changes nothing") {}
      end
      group.run(reporter)

      payloads = frames.select { |f| f.first == :example_finished }.to_h { |f| [f.last[:description], f.last[:metadata]] }
      changed, unchanged = payloads.values_at("changes", "changes nothing")
      expect(changed).to eq(:set => { :added => { :path => "shot.png" }, :tags => %w[a b] }, :deleted => [:gone])
      expect(unchanged).to be_nil
    end

    it "leaves RSpec's own metadata and what the payload carries anyway out of the comparison" do
      group = RSpec.describe("G") do
        it("skips") do |ex|
          ex.metadata[:extra_failure_lines] = ["more"]
          ex.metadata[:last_run_status] = "changed"
          skip "later"
        end
      end
      group.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload[:metadata]).to be_nil
      expect(payload).to include(:skip => true, :pending_message => "later", :extra_failure_lines => ["more"])
    end

    it "names metadata set to values that cannot be marshaled instead of sending them" do
      group = RSpec.describe("G") do
        it("sets a proc") { |ex| ex.metadata[:callback] = lambda {} }
      end
      group.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload[:metadata]).to eq(:unsent => { ":callback" => "Proc" })
    end

    it "copes with values whose `hash` raises and with deleted keys that cannot be marshaled" do
      unhashable = Object.new
      def unhashable.hash
        raise "no hash"
      end
      anonymous_key = Class.new
      group = RSpec.describe("G", anonymous_key => true, :odd => unhashable) do
        it("deletes") { |ex| ex.metadata.delete(anonymous_key) }
      end
      group.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload[:metadata]).to be_nil
    end

    it "forwards metadata a worker listener sets once the example finished" do
      listener = Object.new
      def listener.example_passed(notification)
        notification.example.metadata[:screenshot] = "after.png"
      end
      reporter.register_listener(listener, :example_passed)

      RSpec.describe("G") { it("passes") {} }.run(reporter)

      payload = frames.find { |f| f.first == :example_finished }.last
      expect(payload[:metadata]).to eq(:set => { :screenshot => "after.png" })
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

    it "forwards the expectations counted since it last did, and nothing when there were none" do
      reporter.forward_expectation_count
      3.times { reporter.increment_expectation_count }
      reporter.forward_expectation_count
      reporter.forward_expectation_count
      reporter.increment_expectation_count
      reporter.forward_expectation_count

      expect(frames).to eq([[:expectations, 3], [:expectations, 1]])
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
