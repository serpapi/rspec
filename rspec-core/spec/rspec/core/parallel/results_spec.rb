require 'rspec/core/parallel/coordinator'

module RSpec::Core::Parallel
  RSpec.describe Results do
    let(:example) do
      RSpec.describe("G", :gone => true, :kept => "k") { it("e", :tags => ["a"]) {} }.examples.first
    end
    let(:results) { Results.new }

    def payload(metadata)
      now = RSpec::Core::Parallel.time_to_wire(::Time.now)
      { :status => :passed, :started_at => now, :finished_at => now, :run_time => 0.0, :metadata => metadata }
    end

    it "applies the metadata the worker's run set and deleted when it applies the result" do
      results.record(example, payload(:set => { :tags => %w[a b], :screenshot => "x.png" }, :deleted => [:gone]))
      expect(example.metadata).to include(:gone => true, :tags => ["a"])

      results.apply(example)

      expect(example.metadata).to include(:tags => %w[a b], :screenshot => "x.png", :kept => "k")
      expect(example.metadata).not_to have_key(:gone)
      expect(example.execution_result.status).to eq(:passed)
    end

    it "leaves the metadata alone when the worker's run changed none, or only sent names of unsent values" do
      before = example.metadata.dup
      results.record(example, payload(:unsent => { ":callback" => "Proc" }))
      results.apply(example)

      expect(example.metadata.except(:execution_result)).to eq(before.except(:execution_result))
    end
  end
end
