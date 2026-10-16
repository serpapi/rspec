module RSpec
  module Core
    module Parallel
      # Carries the parts of an exception that formatters consume from a worker
      # process to the coordinator without ever marshaling the exception itself
      # (user exceptions routinely hold IO objects, anonymous classes or large
      # object graphs). The coordinator rebuilds an exception whose class
      # reports the original name -- and, when that class is loaded in the
      # coordinating process, is a subclass of it, so `rescue`-style and
      # `is_a?` checks in formatters keep working.
      # @private
      module RemoteException
        MAX_CAUSE_DEPTH = 10
        CONSTANT_NAME = /\A[A-Z]\w*(::[A-Z]\w*)*\z/

        # @return [Hash, nil] a `Marshal`-safe description of `exception`
        def self.payload_for(exception, depth=0)
          return nil if exception.nil?

          payload = {
            :class_name       => exception.class.name,
            :message          => message_for(exception),
            :detailed_message => detailed_message_for(exception),
            :backtrace        => backtrace_for(exception),
            :cause            => cause_payload_for(exception, depth)
          }

          if MultipleExceptionError::InterfaceTag === exception
            payload[:multiple] = multiple_payload_for(exception, depth)
          end

          payload
        end

        # @return [Exception, nil] an exception rebuilt from `payload_for`'s output
        def self.rebuild(payload)
          return nil unless payload

          exception = class_for(payload).allocate
          ::Exception.instance_method(:initialize).bind_call(exception, payload[:message])
          exception.set_backtrace(payload[:backtrace]) if payload[:backtrace]
          exception.instance_variable_set(:@__rspec_remote_payload, payload)
          exception
        end

        # @private
        def self.message_for(exception)
          exception.message.to_s
        rescue Exception => e # rubocop:disable Lint/RescueException
          "A #{exception.class} for which `exception.message.to_s` raises #{e.class}."
        end

        # @private
        def self.detailed_message_for(exception)
          return nil unless exception.respond_to?(:detailed_message)

          exception.detailed_message(:highlight => false).to_s
        rescue Exception # rubocop:disable Lint/RescueException
          nil
        end

        # @private
        def self.backtrace_for(exception)
          exception.backtrace&.map(&:to_s)
        end

        # @private
        def self.cause_payload_for(exception, depth)
          return nil if depth >= MAX_CAUSE_DEPTH

          cause = exception.cause
          cause && payload_for(cause, depth + 1)
        end

        # @private
        def self.multiple_payload_for(exception, depth)
          {
            :all_exceptions              => exception.all_exceptions.map { |e| payload_for(e, depth + 1) },
            :aggregation_metadata        => Parallel.marshal_safe(exception.aggregation_metadata) do |metadata|
              { :hide_backtrace => !!(metadata && metadata[:hide_backtrace]) }
            end,
            :aggregation_block_label     => exception.aggregation_block_label&.to_s,
            :summary                     => exception.summary.to_s,
            :exception_count_description => exception.exception_count_description.to_s
          }
        end

        # @private
        def self.class_for(payload)
          name = payload[:class_name]
          base = resolve(name) || StandardError
          multiple = payload.key?(:multiple)

          @classes ||= {}
          @classes[[name, base, multiple]] ||= build_class(name, base, multiple)
        end

        # @private
        # Classes defined while loading the suite exist in the coordinator too,
        # since workers are forked from it; classes defined while running an
        # example do not and fall back to a `StandardError` stand-in.
        def self.resolve(name)
          return nil unless name && CONSTANT_NAME =~ name

          klass = Object.const_get(name)
          klass if klass.is_a?(Class) && klass <= ::Exception
        rescue StandardError, ScriptError
          nil
        end

        # @private
        def self.build_class(name, base, multiple)
          Class.new(base) do
            include Remote

            if multiple
              include MultipleExceptionError::InterfaceTag unless base <= MultipleExceptionError::InterfaceTag
              include RemoteMultiple
            end

            define_singleton_method(:name) { name }
            define_singleton_method(:to_s) { name || super() }
            define_singleton_method(:inspect) { name || super() }
          end
        end

        # Instance methods of every rebuilt exception.
        # @private
        module Remote
          BACKTRACE = ::Exception.instance_method(:backtrace)
          BACKTRACE_LOCATIONS = ::Exception.instance_method(:backtrace_locations)
          SET_BACKTRACE = ::Exception.instance_method(:set_backtrace)

          def message
            remote_payload[:message]
          end

          def to_s
            remote_payload[:message]
          end

          def inspect
            "#<#{self.class.name}: #{message}>"
          end

          def detailed_message(*)
            remote_payload[:detailed_message] || message
          end

          def cause
            @cause ||= RemoteException.rebuild(remote_payload[:cause])
          end

          # The original class may keep its backtrace in state the rebuilt
          # instance does not have (`Minitest::UnexpectedError` delegates to
          # the error it wraps), so use the one set from the payload -- or by
          # `set_backtrace` later on.
          def backtrace
            BACKTRACE.bind_call(self)
          end

          def set_backtrace(backtrace) # rubocop:disable Naming/AccessorMethodName
            SET_BACKTRACE.bind_call(self, backtrace)
          end

          def backtrace_locations
            BACKTRACE_LOCATIONS.bind_call(self)
          end

        private

          def remote_payload
            @__rspec_remote_payload
          end
        end

        # Instance methods of rebuilt exceptions that aggregate several failures.
        # @private
        module RemoteMultiple
          def all_exceptions
            @all_exceptions ||= remote_payload[:multiple][:all_exceptions].map do |payload|
              RemoteException.rebuild(payload)
            end
          end

          def failures
            all_exceptions.select { |e| e.class.name =~ /RSpec/ }
          end

          def other_errors
            all_exceptions.reject { |e| e.class.name =~ /RSpec/ }
          end

          def aggregation_metadata
            remote_payload[:multiple][:aggregation_metadata]
          end

          def aggregation_block_label
            remote_payload[:multiple][:aggregation_block_label]
          end

          def summary
            remote_payload[:multiple][:summary]
          end

          def exception_count_description
            remote_payload[:multiple][:exception_count_description]
          end
        end
      end
    end
  end
end
