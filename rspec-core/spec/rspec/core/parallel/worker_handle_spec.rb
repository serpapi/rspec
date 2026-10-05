module RSpec::Core::Parallel
  RSpec.describe WorkerHandle do
    it "closes the pipe it opened when the system refuses it the second one" do
      first = Channel.pipe
      allow(Channel).to receive(:pipe).and_invoke(lambda { first }, lambda { raise Errno::EMFILE })

      expect { WorkerHandle.new(1) }.to raise_error(Errno::EMFILE)
      expect(first.map(&:closed?)).to eq([true, true])
    end
  end
end
