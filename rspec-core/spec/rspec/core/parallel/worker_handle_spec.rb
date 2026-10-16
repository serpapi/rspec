require 'rspec/core/parallel/coordinator'

module RSpec::Core::Parallel
  RSpec.describe WorkerHandle do
    it "closes the pipe it opened when the system refuses it the second one" do
      first = Channel.pipe
      allow(Channel).to receive(:pipe).and_invoke(lambda { first }, lambda { raise Errno::EMFILE })

      expect { WorkerHandle.new(1) }.to raise_error(Errno::EMFILE)
      expect(first.map(&:closed?)).to eq([true, true])
    end

    describe "#signal" do
      let(:handle) { WorkerHandle.new(1).tap { |h| h.pid = 4242 } }
      let(:sent) { [] }

      after { handle.close_all }

      it "sends SIGKILL where SIGTERM cannot be sent (Windows)" do
        process = double(:process)
        allow(process).to receive(:kill) do |name, pid|
          raise Errno::EINVAL if name == 'TERM'

          sent << [name, pid]
        end

        handle.signal(process, 'TERM', :term, 1.0)

        expect(sent).to eq([['KILL', 4242]])
        expect(handle.escalation).to eq(:term)
      end
    end

    describe "#spawn_worker" do
      it "starts the command with its jobs on standard input and its results on standard output" do
        handle = WorkerHandle.new(2)
        process = double(:process)
        expect(process).to receive(:spawn).with({ "A" => "1" }, "ruby", "rspec", :in => handle.child_jobs.io,
                                                :out => handle.child_results.io, :chdir => Dir.pwd).and_return(4242)

        expect(handle.spawn_worker(process, %w[ruby rspec], "A" => "1")).to eq(4242)
        handle.close_all
      end
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
