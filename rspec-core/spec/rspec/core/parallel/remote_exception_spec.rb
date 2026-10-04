module RSpec::Core::Parallel
  RSpec.describe RemoteException do
    include FormatterSupport

    def round_trip(exception)
      payload = Marshal.load(Marshal.dump(RemoteException.payload_for(exception)))
      RemoteException.rebuild(payload)
    end

    def raise_with_cause
      begin
        raise "the cause"
      rescue
        raise ArgumentError, "the effect"
      end
    rescue ArgumentError => e
      e
    end

    it "returns nil for nil" do
      expect(RemoteException.payload_for(nil)).to be_nil
      expect(RemoteException.rebuild(nil)).to be_nil
    end

    it "rebuilds an exception whose class reports the original name and is a subclass of the real class" do
      original = begin
        raise ArgumentError, "bad arg"
      rescue ArgumentError => e
        e
      end

      rebuilt = round_trip(original)

      expect(rebuilt).to be_a(ArgumentError)
      expect(rebuilt.class).not_to equal(ArgumentError)
      expect(rebuilt.class.name).to eq("ArgumentError")
      expect(rebuilt.class.to_s).to eq("ArgumentError")
      expect(rebuilt.class.inspect).to eq("ArgumentError")
      expect(rebuilt.message).to eq("bad arg")
      expect(rebuilt.to_s).to eq("bad arg")
      expect(rebuilt.inspect).to eq("#<ArgumentError: bad arg>")
      expect(rebuilt.backtrace).to eq(original.backtrace)
      expect(rebuilt.cause).to be_nil
    end

    it "reuses one generated class per original class" do
      a = round_trip(ArgumentError.new("a"))
      b = round_trip(ArgumentError.new("b"))

      expect(a.class).to equal(b.class)
    end

    it "rebuilds the cause chain" do
      rebuilt = round_trip(raise_with_cause)

      expect(rebuilt.cause).to be_a(RuntimeError)
      expect(rebuilt.cause.message).to eq("the cause")
      expect(rebuilt.cause.cause).to be_nil
    end

    it "bounds the depth of the cause chain it ships" do
      exception = (1..(RemoteException::MAX_CAUSE_DEPTH + 5)).inject(nil) do |cause, i|
        error = RuntimeError.new("level #{i}")
        error.instance_variable_set(:@__cause, cause)
        error.define_singleton_method(:cause) { @__cause }
        error
      end

      payload = RemoteException.payload_for(exception)
      depth = 0
      while payload
        depth += 1
        payload = payload[:cause]
      end
      expect(depth).to eq(RemoteException::MAX_CAUSE_DEPTH + 1)
    end

    it "falls back to a StandardError stand-in named like a class that does not exist here" do
      original = Class.new(StandardError) { def self.name = "Only::InWorker" }.new("gone")
      rebuilt = round_trip(original)

      expect(rebuilt).to be_a(StandardError)
      expect(rebuilt.class.name).to eq("Only::InWorker")
      expect(rebuilt.message).to eq("gone")
    end

    it "does not resolve names that are not constant paths or not exception classes" do
      expect(RemoteException.resolve("String")).to be_nil
      expect(RemoteException.resolve("not a constant")).to be_nil
      expect(RemoteException.resolve("Kernel")).to be_nil
      expect(RemoteException.resolve(nil)).to be_nil
      expect(RemoteException.resolve("RSpec::Core::ExampleGroup")).to be_nil
      expect(RemoteException.resolve("RSpec::Expectations::ExpectationNotMetError")).to eq(RSpec::Expectations::ExpectationNotMetError)
    end

    it "rescues errors raised while resolving a constant" do
      stub_const("Explosive", Module.new { def self.const_missing(_) = raise("boom") })
      expect(RemoteException.resolve("Explosive::Nope")).to be_nil
    end

    it "preserves an anonymous error class as such" do
      rebuilt = round_trip(Class.new(StandardError).new("anon"))

      expect(rebuilt.class.name).to be_nil
      expect(rebuilt.class.to_s).to match(/#<Class:/)
      expect(rebuilt.class.inspect).to match(/#<Class:/)
      expect(RSpec::Core::Formatters::ExceptionPresenter.new(rebuilt, new_example).message_lines).to include("(anonymous error class):")
    end

    it "does not call the original class's constructor or accessors when rebuilding" do
      klass = Class.new(StandardError) do
        def self.name = "Needs::Arguments"
        def initialize(required, other:) = super("#{required} #{other}")
        def message = "from accessor #{@required.upcase}"
      end
      stub_const("Needs::Arguments", klass)
      original = klass.new("a", :other => "b")
      payload = RemoteException.payload_for(original)
      expect(payload[:message]).to eq("A Needs::Arguments for which `exception.message.to_s` raises NoMethodError.")

      rebuilt = RemoteException.rebuild(payload)
      expect(rebuilt).to be_a(klass)
      expect(rebuilt.message).to eq("A Needs::Arguments for which `exception.message.to_s` raises NoMethodError.")
    end

    it "ships the detailed message so that SyntaxError formatting matches", :if => SyntaxError.method_defined?(:detailed_message) do
      original = begin
        eval("}", binding, __FILE__, __LINE__)
      rescue SyntaxError => e
        e
      end

      rebuilt = round_trip(original)
      expect(rebuilt).to be_a(SyntaxError)
      expect(rebuilt.detailed_message).to eq(original.detailed_message(:highlight => false))
    end

    it "falls back to the message when no detailed message was shipped" do
      rebuilt = RemoteException.rebuild(:class_name => "RuntimeError", :message => "plain", :backtrace => nil, :cause => nil)
      expect(rebuilt.detailed_message).to eq("plain")
      expect(rebuilt.backtrace).to be_nil
    end

    it "tolerates a detailed_message that raises" do
      original = RuntimeError.new("ok")
      def original.detailed_message(*) = raise("nope")
      expect(RemoteException.payload_for(original)[:detailed_message]).to be_nil
    end

    it "keeps `set_backtrace` working on the rebuilt exception so common backtrace truncation applies" do
      rebuilt = round_trip(RuntimeError.new("x").tap { |e| e.set_backtrace(%w[a b c]) })
      copy = rebuilt.dup
      copy.set_backtrace(%w[a])

      expect(copy.backtrace).to eq(%w[a])
      expect(rebuilt.backtrace).to eq(%w[a b c])
    end

    context "with an exception that aggregates several failures" do
      def aggregate_failures_error
        RSpec::Expectations::FailureAggregator.new("checking things", :hide_backtrace => true).aggregate do
          expect(1).to eq(2)
          expect(3).to eq(4)
          raise "boom"
        end
      rescue RSpec::Expectations::MultipleExpectationsNotMetError => e
        e
      end

      it "rebuilds the multiple exception interface" do
        original = aggregate_failures_error
        rebuilt = round_trip(original)

        expect(RSpec::Core::MultipleExceptionError::InterfaceTag === rebuilt).to be true
        expect(rebuilt).to be_a(RSpec::Expectations::MultipleExpectationsNotMetError)
        expect(rebuilt.all_exceptions.map(&:message)).to eq(original.all_exceptions.map(&:message))
        expect(rebuilt.failures.size).to eq(original.failures.size)
        expect(rebuilt.other_errors.map { |e| e.class.name }).to eq(["RuntimeError"])
        expect(rebuilt.aggregation_metadata).to eq(original.aggregation_metadata)
        expect(rebuilt.aggregation_block_label).to eq(original.aggregation_block_label)
        expect(rebuilt.summary).to eq(original.summary)
        expect(rebuilt.exception_count_description).to eq(original.exception_count_description)
      end

      it "gives the stand-in the interface when the class does not exist here" do
        payload = RemoteException.payload_for(RSpec::Core::MultipleExceptionError.new(RuntimeError.new("a")))
        payload[:class_name] = "Unknown::Aggregate"

        rebuilt = RemoteException.rebuild(payload)
        expect(RSpec::Core::MultipleExceptionError::InterfaceTag === rebuilt).to be true
        expect(rebuilt.all_exceptions.map(&:message)).to eq(["a"])
      end

      it "replaces aggregation metadata that cannot be marshaled with the backtrace flag only" do
        original = RSpec::Core::MultipleExceptionError.new(RuntimeError.new("a"))
        original.aggregation_metadata[:proc] = lambda {}

        payload = RemoteException.payload_for(original)
        expect(payload[:multiple][:aggregation_metadata]).to eq(:hide_backtrace => true)
      end
    end

    describe "presenter parity" do
      def formatted(exception)
        example = new_example(:description => "the example")
        example.metadata[:absolute_file_path] = __FILE__
        example.execution_result.status = :failed
        example.execution_result.exception = exception
        RSpec::Core::Formatters::ExceptionPresenter::Factory.new(example).build.fully_formatted(1)
      end

      it "produces the same fully formatted failure for a plain exception with a cause" do
        original = raise_with_cause
        expect(formatted(round_trip(original))).to eq(formatted(original))
      end

      it "produces the same fully formatted failure for an expectation failure" do
        original = begin
          expect(1).to eq(2)
        rescue RSpec::Expectations::ExpectationNotMetError => e
          e
        end

        expect(formatted(round_trip(original))).to eq(formatted(original))
        expect(formatted(original)).to include("Failure/Error")
        expect(formatted(original)).not_to include("ExpectationNotMetError")
      end

      it "produces the same fully formatted failure for aggregated failures" do
        original = RSpec::Core::MultipleExceptionError.new(
          RuntimeError.new("first").tap { |e| e.set_backtrace(["./spec/a_spec.rb:1", "./spec/a_spec.rb:9"]) },
          RSpec::Expectations::ExpectationNotMetError.new("second").tap { |e| e.set_backtrace(["./spec/a_spec.rb:2", "./spec/a_spec.rb:9"]) }
        )
        original.set_backtrace(["./spec/a_spec.rb:9"])

        rebuilt = round_trip(original)
        expect(formatted(rebuilt)).to eq(formatted(original))
        expect(formatted(rebuilt)).to include("Got 1 failure and 1 other error")
      end
    end
  end
end
