module RSpec
  module Core
    module Parallel
      # The results workers report, kept from the moment they arrive -- so
      # failure counting and `.rspec_status` persistence never wait for
      # presentation -- and copied onto the examples when their notifications
      # are delivered (or at the end of the run for whatever was not).
      # @private
      class Results
        attr_reader :failed_count

        def initialize
          @payloads = {}
          @failed_count = 0
        end

        def record(example, payload)
          @payloads[example] = payload
          @failed_count += 1 if payload[:status] == :failed
        end

        def apply(example)
          payload = @payloads.delete(example)
          return unless payload

          example.assign_execution_result(
            payload.merge(
              :started_at        => Parallel.time_from_wire(payload[:started_at]),
              :finished_at       => Parallel.time_from_wire(payload[:finished_at]),
              :exception         => RemoteException.rebuild(payload[:exception]),
              :pending_exception => RemoteException.rebuild(payload[:pending_exception])
            )
          )
        end

        def apply_all
          @payloads.keys.each { |example| apply(example) }
        end
      end
    end
  end
end
