module RSpec
  module Core
    module Parallel
      # Replays worker notifications onto the coordinator's reporter.
      #
      # Formatters that follow example group boundaries (documentation, HTML,
      # ...) need every notification of a top-level group to arrive between
      # that group's `example_group_started` and `example_group_finished`.
      # With several workers running at once this holds only if their streams
      # are interleaved at group boundaries, so when such a formatter is
      # registered one in-flight group at a time is "live" and delivered as it
      # happens while the others are queued and replayed whole once they
      # finish -- the output then reads like a serial run of the groups in
      # completion order. Formatters that do not care about group boundaries
      # (progress, JSON, failures, ...) get every notification immediately.
      #
      # Liveness is bounded in both modes: when the live group has been silent
      # for {SILENCE_BOUND} seconds, when a finished group has been waiting
      # for {WAIT_BOUND} seconds behind a live group that keeps producing
      # output, or when more than {QUEUE_BOUND} notifications or
      # {QUEUE_BYTES_BOUND} bytes of them are queued, waiting notifications
      # are delivered right away. Formatters then see
      # the live group closed and re-opened around them (its headings
      # repeat), which is the price of showing results promptly.
      #
      # Only presentation is sequenced: results, failure counts and example
      # statuses are recorded by the coordinator as they arrive.
      # @private
      class Sequencer
        SILENCE_BOUND = 2.0
        WAIT_BOUND = 10.0
        QUEUE_BOUND = 5_000
        QUEUE_BYTES_BOUND = 8 * 1024 * 1024
        CONTEXT_FREE = [:message, :deprecation, :custom].freeze
        # Shown under the groups it was printed in; outside any, as it comes.
        OUTSIDE_GROUPS = (CONTEXT_FREE + [:output]).freeze

        Event = Struct.new(:kind, :target, :payload, :bytes)

        # The notifications of one top-level group run by one worker.
        # @private
        class Stream
          attr_reader :worker, :events, :open_groups
          attr_accessor :depth, :complete, :completed_at

          def initialize(worker)
            @worker = worker
            @events = []
            @open_groups = []
            @depth = 0
            @complete = false
            @completed_at = nil
          end
        end

        attr_reader :stats

        def initialize(reporter, options={})
          @reporter = reporter
          @apply_result = options.fetch(:apply_result)
          @write_output = options.fetch(:output) { lambda { |_stream, _bytes| } }
          @clock = options.fetch(:clock) { Parallel.method(:monotonic_time) }
          @sequenced = options.fetch(:sequenced) { self.class.group_aware_listeners?(reporter) }
          @streams = {}
          @completed = []
          @live = nil
          @visible = nil
          @visible_groups = []
          @last_delivery_at = @clock.call
          @group_durations = {}
          @stats = { :queued => 0, :peak_queued => 0, :queued_bytes => 0, :peak_queued_bytes => 0, :flushes => 0 }
        end

        # @return [Boolean] whether a registered formatter reacts to group
        #   boundaries. `BaseFormatter#example_group_started` only records the
        #   current group and RSpec's profiler keeps its own per-group tally,
        #   so neither needs sequencing.
        def self.group_aware_listeners?(reporter)
          # Not loaded when every formatter is a custom one that does not inherit it.
          base_formatter = Formatters::BaseFormatter if defined?(Formatters::BaseFormatter)
          [:example_group_started, :example_group_finished].any? do |event|
            reporter.registered_listeners(event).any? do |listener|
              !listener.is_a?(Profiler) && listener.method(event).owner != base_formatter
            end
          end
        end

        def sequenced?
          @sequenced
        end

        def group_started(worker, group, bytes=0)
          push(worker, Event.new(:group_started, group, nil, bytes)) { |stream| stream.depth += 1 }
        end

        def group_finished(worker, group, duration, bytes=0)
          push(worker, Event.new(:group_finished, group, duration, bytes)) do |stream|
            stream.depth -= 1
            stream.complete = true if stream.depth <= 0
          end
        end

        def example_started(worker, example, bytes=0)
          push(worker, Event.new(:example_started, example, nil, bytes))
        end

        def example_finished(worker, example, bytes=0)
          push(worker, Event.new(:example_finished, example, nil, bytes))
        end

        def message(worker, message, bytes=0)
          push(worker, Event.new(:message, nil, message, bytes))
        end

        def deprecation(worker, hash, bytes=0)
          push(worker, Event.new(:deprecation, nil, hash, bytes))
        end

        def custom(worker, event, options, bytes=0)
          push(worker, Event.new(:custom, event, options, bytes))
        end

        # What the worker printed to `stream` (`:out` or `:err`).
        def output(worker, stream, bytes)
          push(worker, Event.new(:output, stream, bytes, bytes.bytesize))
        end

        # The worker died (or was stopped) in the middle of a group: deliver
        # what it reported and balance the group notifications formatters saw.
        def abandon(worker)
          stream = @streams.delete(worker)
          return unless stream

          replay(stream)
          close_visible if @visible.equal?(stream)
          @live = nil if @live.equal?(stream)
          promote
        end

        # Called periodically by the coordinator.
        def tick(now=@clock.call)
          return unless @sequenced
          return unless @completed.any?

          flush_completed if now - @last_delivery_at >= SILENCE_BOUND || now - @completed.first.completed_at >= WAIT_BOUND
        end

        # Delivers everything still queued and closes any open group.
        def finish
          flush_everything
          @streams.keys.each { |worker| abandon(worker) }
          close_visible
        end

      private

        def push(worker, event)
          stream = @streams[worker]

          # Messages sent outside any group (suite hooks) are not sequenced.
          if OUTSIDE_GROUPS.include?(event.kind) && (stream.nil? || stream.depth <= 0)
            return deliver(Stream.new(worker), event)
          end

          stream ||= (@streams[worker] = Stream.new(worker))
          yield stream if block_given?

          if live?(stream)
            deliver(stream, event)
          else
            enqueue(stream, event)
          end

          complete(stream) if stream.complete
        end

        def live?(stream)
          return true unless @sequenced

          @live = stream if @live.nil?
          @live.equal?(stream)
        end

        def complete(stream)
          @streams.delete(stream.worker)

          if stream.events.empty?
            @live = nil if @live.equal?(stream)
            promote
          else
            stream.completed_at = @clock.call
            @completed << stream
          end
        end

        def enqueue(stream, event)
          stream.events << event
          @stats[:queued] += 1
          @stats[:queued_bytes] += event.bytes
          @stats[:peak_queued] = [@stats[:peak_queued], @stats[:queued]].max
          @stats[:peak_queued_bytes] = [@stats[:peak_queued_bytes], @stats[:queued_bytes]].max
          # At once: output read in chunks between two frames must not pile up.
          flush_everything if @stats[:queued] >= QUEUE_BOUND || @stats[:queued_bytes] >= QUEUE_BYTES_BOUND
        end

        def deliver(stream, event)
          if CONTEXT_FREE.include?(event.kind)
            deliver_context_free(event)
          elsif event.kind == :output && stream.open_groups.empty?
            @write_output.call(event.target, event.payload)
          else
            switch_to(stream) unless @visible.equal?(stream)
            deliver_in_context(stream, event)
          end

          @last_delivery_at = @clock.call
        end

        def deliver_in_context(stream, event)
          case event.kind
          when :group_started
            @reporter.example_group_started(event.target)
            stream.open_groups.push(event.target)
            @visible_groups.push(event.target)
          when :group_finished
            @reporter.example_group_finished(event.target)
            stream.open_groups.pop
            @visible_groups.pop
            record_duration(event.target, event.payload) if event.payload
          when :example_started
            @reporter.example_started(event.target)
          when :output
            @write_output.call(event.target, event.payload)
          when :example_finished
            @apply_result.call(event.target)
            @reporter.example_finished(event.target)
            @reporter.__send__(:"example_#{event.target.execution_result.status}", event.target)
          end
        end

        # Measured in the worker that ran it; a group split across workers
        # took as long as its parts did together.
        def record_duration(group, duration)
          total = @group_durations[group] = @group_durations.fetch(group, 0.0) + duration
          @reporter.record_example_group_duration(group, total)
        end

        def deliver_context_free(event)
          case event.kind
          when :message     then @reporter.message(event.payload)
          when :deprecation then @reporter.deprecation(event.payload)
          when :custom      then @reporter.publish(event.target, event.payload)
          end
        end

        def replay(stream)
          until stream.events.empty?
            event = stream.events.shift
            @stats[:queued] -= 1
            @stats[:queued_bytes] -= event.bytes
            deliver(stream, event)
          end
        end

        def switch_to(stream)
          close_visible
          stream.open_groups.each { |group| @reporter.example_group_started(group) }
          @visible_groups = stream.open_groups.dup
          @visible = stream
        end

        def close_visible
          @visible_groups.reverse_each { |group| @reporter.example_group_finished(group) }
          @visible_groups = []
          @visible = nil
        end

        # The live group finished: show the groups that finished meanwhile,
        # then let the longest-waiting in-flight group go live.
        def promote
          flush_completed
          return unless @live.nil?

          waiting = @streams.values.select { |stream| stream.events.any? }
          return if waiting.empty?

          @live = waiting.first
          replay(@live)
        end

        def flush_completed
          @stats[:flushes] += 1 if @completed.any?
          while (stream = @completed.shift)
            replay(stream)
          end
        end

        def flush_everything
          flush_completed
          @stats[:flushes] += 1 if @streams.values.any? { |stream| stream.events.any? }
          @streams.values.each { |stream| replay(stream) }
        end
      end
    end
  end
end
