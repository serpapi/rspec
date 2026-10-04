module RSpec::Core::Parallel
  class RecordingListener
    attr_reader :events

    def initialize
      @events = []
    end

    RSpec::Core::Reporter::RSPEC_NOTIFICATIONS.each do |name|
      define_method(name) { |notification| @events << [name, label(notification)] }
    end

    def my_event(notification)
      @events << [:my_event, notification.to_h]
    end

  private

    def label(notification)
      if notification.respond_to?(:group)
        notification.group.description
      elsif notification.respond_to?(:example)
        notification.example.description
      elsif notification.respond_to?(:message)
        notification.message
      end
    end
  end

  RSpec.describe Sequencer do
    let(:reporter) { RSpec::Core::Reporter.new(RSpec.configuration) }
    let(:listener) { RecordingListener.new }
    let(:applied) { [] }
    let(:now) { [0.0] }
    let(:clock) { lambda { now.first } }

    let(:g1) do
      RSpec.describe("G1") do
        it("a") {}
        context("N") { it("b") {} }
      end
    end
    let(:g2) { RSpec.describe("G2") { it("c") {} } }
    let(:g3) { RSpec.describe("G3") { it("d") {} } }

    def ex(group, description)
      (group.examples + group.children.flat_map(&:examples)).find { |e| e.description == description }
    end

    def sequencer(sequenced)
      reporter.register_listener(listener, *RSpec::Core::Reporter::RSPEC_NOTIFICATIONS, :my_event)
      Sequencer.new(reporter, :apply_result => lambda { |example| applied << example }, :clock => clock, :sequenced => sequenced)
    end

    def run_example(seq, worker, example, status=:passed)
      example.execution_result.status = status
      seq.example_started(worker, example)
      seq.example_finished(worker, example)
    end

    def run_group(seq, worker, group, duration=1.5)
      seq.group_started(worker, group)
      group.examples.each { |example| run_example(seq, worker, example) }
      group.children.each { |child| run_group(seq, worker, child, nil) }
      seq.group_finished(worker, group, duration)
    end

    def events
      listener.events
    end

    describe "immediate mode (no formatter follows group boundaries)" do
      it "delivers every notification as it arrives, re-emitting group boundaries so the current group is always right" do
        seq = sequencer(false)

        seq.group_started(1, g1)
        seq.group_started(2, g2)
        run_example(seq, 1, ex(g1, "a"))
        run_example(seq, 2, ex(g2, "c"), :failed)
        seq.group_finished(2, g2, 0.5)
        seq.group_finished(1, g1, 1.0)

        expect(events).to eq([
          [:example_group_started, "G1"],
          [:example_group_finished, "G1"], [:example_group_started, "G2"],
          [:example_group_finished, "G2"], [:example_group_started, "G1"],
          [:example_started, "a"], [:example_finished, "a"], [:example_passed, "a"],
          [:example_group_finished, "G1"], [:example_group_started, "G2"],
          [:example_started, "c"], [:example_finished, "c"], [:example_failed, "c"],
          [:example_group_finished, "G2"],
          [:example_group_started, "G1"],
          [:example_group_finished, "G1"]
        ])
        expect(seq.stats[:peak_queued]).to eq(0)
        expect(seq).not_to be_sequenced
      end

      it "applies the result right before delivering example_finished" do
        seq = sequencer(false)
        seq.group_started(1, g1)
        run_example(seq, 1, ex(g1, "a"))

        expect(applied).to eq([ex(g1, "a")])
      end

      it "records the worker-measured duration of top-level groups for the profiler" do
        seq = sequencer(false)
        expect(reporter).to receive(:record_example_group_duration).with(g1, 1.5)
        expect(reporter).not_to receive(:record_example_group_duration).with(g1.children.first, anything)

        run_group(seq, 1, g1)
      end
    end

    describe "sequenced mode" do
      it "streams the first group live and replays the others whole, in completion order, once it finishes" do
        seq = sequencer(true)

        seq.group_started(1, g1)
        seq.group_started(2, g2)
        seq.group_started(3, g3)
        run_example(seq, 2, ex(g2, "c"))
        run_example(seq, 1, ex(g1, "a"))
        run_example(seq, 3, ex(g3, "d"))
        seq.group_finished(3, g3, 0.1)
        seq.group_finished(2, g2, 0.2)
        expect(events.map(&:last)).not_to include("G2", "G3", "c", "d")
        expect(seq.stats[:queued]).to eq(8)
        expect(seq.stats[:peak_queued]).to eq(8)

        seq.group_started(1, g1.children.first)
        run_example(seq, 1, ex(g1, "b"))
        seq.group_finished(1, g1.children.first, nil)
        seq.group_finished(1, g1, 0.3)

        expect(events).to eq([
          [:example_group_started, "G1"],
          [:example_started, "a"], [:example_finished, "a"], [:example_passed, "a"],
          [:example_group_started, "N"],
          [:example_started, "b"], [:example_finished, "b"], [:example_passed, "b"],
          [:example_group_finished, "N"],
          [:example_group_finished, "G1"],
          [:example_group_started, "G3"],
          [:example_started, "d"], [:example_finished, "d"], [:example_passed, "d"],
          [:example_group_finished, "G3"],
          [:example_group_started, "G2"],
          [:example_started, "c"], [:example_finished, "c"], [:example_passed, "c"],
          [:example_group_finished, "G2"]
        ])
        expect(seq.stats[:queued]).to eq(0)
        expect(seq.stats[:flushes]).to eq(1)
        expect(seq).to be_sequenced
      end

      it "promotes the longest-waiting unfinished group when the live one finishes and streams it live from then on" do
        seq = sequencer(true)

        seq.group_started(1, g1)
        seq.group_started(2, g2)
        seq.group_started(3, g3)
        seq.group_finished(1, g1, 0.1)

        expect(events).to eq([
          [:example_group_started, "G1"], [:example_group_finished, "G1"],
          [:example_group_started, "G2"]
        ])

        run_example(seq, 3, ex(g3, "d"))
        run_example(seq, 2, ex(g2, "c"))
        expect(events.last).to eq([:example_passed, "c"])
        expect(events.map(&:last)).not_to include("d")
      end

      it "treats a group with no queued notifications as finished when its final notification arrives live" do
        seq = sequencer(true)
        run_group(seq, 1, g2)
        run_group(seq, 2, g3)

        expect(events.map(&:last)).to eq(%w[G2 c c c G2 G3 d d d G3])
      end

      it "flushes finished groups when the live group has been silent for #{Sequencer::SILENCE_BOUND} seconds" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        run_group(seq, 2, g2)
        expect(events.map(&:last)).to eq(%w[G1])

        now[0] = Sequencer::SILENCE_BOUND - 0.1
        seq.tick
        expect(events.map(&:last)).to eq(%w[G1])

        now[0] = Sequencer::SILENCE_BOUND
        seq.tick
        expect(events.map(&:last)).to eq(%w[G1 G1 G2 c c c G2])

        run_example(seq, 1, ex(g1, "a"))
        expect(events.map(&:last)[7..-1]).to eq(%w[G1 a a a])
        expect(seq.stats[:flushes]).to eq(1)
      end

      it "flushes finished groups once one has waited #{Sequencer::WAIT_BOUND} seconds behind a live group that keeps talking" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        run_group(seq, 2, g2)

        (1..Sequencer::WAIT_BOUND.to_i).each do |second|
          now[0] = second.to_f
          run_example(seq, 1, ex(g1, "a"))
          seq.tick
          break if events.map(&:last).include?("G2")
        end

        expect(now.first).to eq(Sequencer::WAIT_BOUND)
        expect(events.map(&:last)).to include("G2", "c")
      end

      it "does nothing on tick while nothing finished is waiting" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.group_started(2, g2)
        now[0] = 100.0

        expect { seq.tick }.not_to(change { events })
      end

      it "falls back to delivering everything queued, finished or not, past #{Sequencer::QUEUE_BOUND} notifications" do
        stub_const("RSpec::Core::Parallel::Sequencer::QUEUE_BOUND", 3)
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.group_started(2, g2)
        run_example(seq, 2, ex(g2, "c"))
        expect(events.map(&:last)).to eq(%w[G1])

        seq.tick
        expect(events.map(&:last)).to eq(%w[G1 G1 G2 c c c])
        expect(seq.stats[:queued]).to eq(0)

        seq.group_finished(2, g2, 0.1)
        expect(seq.stats[:queued]).to eq(1)
      end

      it "delivers messages sent outside any group immediately, without touching the open group" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.message(2, "from a suite hook")
        seq.deprecation(2, :message => "old", :deprecated => "x", :replacement => nil, :call_site => nil)
        seq.custom(2, :my_event, :a => 1)

        expect(events).to eq([
          [:example_group_started, "G1"],
          [:message, "from a suite hook"],
          [:deprecation, "old"],
          [:my_event, { :a => 1 }]
        ])
      end

      it "queues messages sent from inside a queued group with that group" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.group_started(2, g2)
        seq.message(2, "inside G2")
        expect(events.map(&:last)).to eq(%w[G1])

        seq.group_finished(2, g2, 0.1)
        seq.group_finished(1, g1, 0.1)
        expect(events.map(&:last)).to eq(["G1", "G1", "G2", "inside G2", "G2"])
      end

      it "tracks the bytes of queued frames" do
        seq = sequencer(true)
        seq.group_started(1, g1, 10)
        seq.group_started(2, g2, 20)
        seq.example_started(2, ex(g2, "c"), 30)

        expect(seq.stats).to include(:queued => 2, :queued_bytes => 50, :peak_queued_bytes => 50)
      end
    end

    describe "#abandon" do
      it "balances the group notifications of a live group that will never finish" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.group_started(1, g1.children.first)

        seq.abandon(1)

        expect(events.map(&:last)).to eq(%w[G1 N N G1])
        seq.group_started(2, g2)
        expect(events.last).to eq([:example_group_started, "G2"])
      end

      it "delivers what a queued group reported before closing it" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.group_started(2, g2)
        run_example(seq, 2, ex(g2, "c"))

        seq.abandon(2)

        expect(events.map(&:last)).to eq(%w[G1 G1 G2 c c c G2])
      end

      it "ignores workers without a stream" do
        seq = sequencer(true)
        expect { seq.abandon(9) }.not_to(change { events })
      end
    end

    describe "#finish" do
      it "delivers everything queued and closes the groups still open" do
        seq = sequencer(true)
        seq.group_started(1, g1)
        seq.group_started(2, g2)
        run_example(seq, 2, ex(g2, "c"))
        seq.group_started(3, g3)
        seq.group_finished(3, g3, 0.1)

        seq.finish

        expect(events.map(&:last)).to eq(%w[G1 G1 G3 G3 G2 c c c G2])
        expect(seq.stats[:queued]).to eq(0)
      end
    end

    describe ".group_aware_listeners?" do
      def reporter_with(formatter, *args)
        RSpec.configuration.add_formatter(formatter, *args)
        RSpec.configuration.reporter
      end

      it "is false for formatters that only inherit BaseFormatter's bookkeeping, like progress and JSON" do
        expect(Sequencer.group_aware_listeners?(reporter_with("progress", StringIO.new))).to be false
        expect(Sequencer.group_aware_listeners?(reporter_with("json", StringIO.new))).to be false
      end

      it "is true for the documentation and HTML formatters" do
        expect(Sequencer.group_aware_listeners?(reporter_with("documentation", StringIO.new))).to be true
      end

      it "is true for the HTML formatter, which reacts to group starts" do
        expect(Sequencer.group_aware_listeners?(reporter_with("html", StringIO.new))).to be true
      end

      it "is true for a custom listener of group finishes" do
        reporter.register_listener(Class.new { def example_group_finished(_); end }.new, :example_group_finished)
        expect(Sequencer.group_aware_listeners?(reporter)).to be true
      end

      it "ignores RSpec's own profiler" do
        reporter.register_listener(RSpec::Core::Profiler.new, :example_group_started, :example_group_finished)
        expect(Sequencer.group_aware_listeners?(reporter)).to be false
      end

      it "is consulted by default" do
        reporter.register_listener(Class.new { def example_group_finished(_); end }.new, :example_group_finished)
        expect(Sequencer.new(reporter, :apply_result => lambda { |_| })).to be_sequenced
      end
    end
  end
end
