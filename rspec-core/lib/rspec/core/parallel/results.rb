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
          @unsent_metadata = {}
        end

        def record(example, payload)
          @payloads[example] = payload
          @failed_count += 1 if payload[:status] == :failed
        end

        def apply(example)
          payload = @payloads.delete(example)
          return unless payload

          apply_metadata(example.metadata, payload[:metadata])
          example.assign_execution_result(
            payload.merge(
              :started_at        => Parallel.time_from_wire(payload[:started_at]),
              :finished_at       => Parallel.time_from_wire(payload[:finished_at]),
              :exception         => RemoteException.rebuild(payload[:exception]),
              :pending_exception => RemoteException.rebuild(payload[:pending_exception])
            )
          )
        end

        # What a worker sent of an example's metadata changes, loaded one by
        # one: a value whose class is not loaded here is left out -- with a
        # warning, once per key -- like one the worker could not marshal.
        # @return [Hash, nil] the keys set (`:set`) and deleted (`:deleted`)
        def metadata_changes(worker_description, changes)
          return nil unless changes

          unsent = changes.fetch(:unsent, {}).dup
          load = lambda do |(key, dump)|
            # rubocop:disable-next Security/MarshalLoad
            Marshal.load(dump) # dumped by the worker's own reporter
          rescue StandardError => e
            unsent[key] = "it cannot be loaded here (#{e.class}: #{e.message[0, 200]})"
            nil
          end
          set = changes.fetch(:set, []).map(&load).compact.to_h
          deleted = changes.fetch(:deleted, []).map(&load).compact
          warn_unsent_metadata(worker_description, unsent)
          { :set => set, :deleted => deleted }
        end

        # What the example's run in a worker did to its metadata, done to the
        # coordinator's copy before formatters are told about it.
        def apply_metadata(metadata, changes)
          return unless changes

          changes.fetch(:set, {}).each { |key, value| metadata[key] = value }
          changes.fetch(:deleted, []).each { |key| metadata.delete(key) }
        end

        def apply_all
          @payloads.keys.each { |example| apply(example) }
        end

      private

        def warn_unsent_metadata(worker_description, unsent)
          unsent.each do |key, reason|
            next if @unsent_metadata.key?(key)

            @unsent_metadata[key] = true
            RSpec.warning "#{worker_description} set example metadata #{key} to a value that cannot reach the " \
                          "formatters (#{reason}), so they see the value it had before the example ran. Later such " \
                          "values of #{key} are not reported", :call_site => nil
          end
        end
      end
    end
  end
end
