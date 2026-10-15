require 'rspec/core/parallel/coordinator'

module RSpec::Core::Parallel
  RSpec.describe WorkerHandle do
    it "closes the pipe it opened when the system refuses it the second one" do
      first = Channel.pipe
      allow(Channel).to receive(:pipe).and_invoke(lambda { first }, lambda { raise Errno::EMFILE })

      expect { WorkerHandle.new(1) }.to raise_error(Errno::EMFILE)
      expect(first.map(&:closed?)).to eq([true, true])
    end

    describe "the deadline for its first frame" do
      let(:handle) { WorkerHandle.new(1) }

      after { handle.close_all }

      it "is cleared by its first frame, postponed or not" do
        handle.start_deadline = 5.0
        handle.postpone(1.0)
        handle.clear_start_deadline

        expect(handle.deadline).to be_nil
      end

      it "is replaced by any other deadline, which its first frame then leaves alone" do
        handle.start_deadline = 5.0
        handle.deadline = 2.0 # asked to stop early before it reported back, say

        handle.clear_start_deadline

        expect(handle.deadline).to eq(2.0)
      end
    end
  end
end
