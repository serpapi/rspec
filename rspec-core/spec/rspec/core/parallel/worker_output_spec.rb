require 'fileutils'
require 'tmpdir'
require 'rspec/core/parallel/coordinator'

# These specs stand in for the process's own streams, which a worker moves.
# rubocop:disable-next Style/GlobalStdStream
module RSpec::Core::Parallel
  RSpec.describe WorkerOutput do
    let!(:dir) { Dir.mktmpdir }
    let!(:output) { WorkerOutput.new(dir, 3) }

    after do
      output.close
      FileUtils.remove_entry(dir)
    end

    def read(sizes=nil)
      chunks = []
      output.read(sizes) { |stream, bytes| chunks << [stream, bytes] }
      chunks
    end

    it "creates an empty file per stream, named after the worker" do
      expect(output.paths.values.map { |path| File.basename(path) }).to eq(%w[worker-3.out worker-3.err])
      expect(output.paths.values.map { |path| File.size(path) }).to eq([0, 0])
    end

    it "yields what was written up to the given sizes, each byte once, and the rest when asked for everything" do
      File.write(output.paths[:out], "one two")
      File.write(output.paths[:err], "warn")

      expect(read(:out => 3, :err => 0)).to eq([[:out, "one"]])
      expect(read(:out => 3, :err => 0)).to eq([])
      expect(read).to eq([[:out, " two"], [:err, "warn"]])
      expect(read).to eq([])
    end

    it "moves past what it was told to skip" do
      File.write(output.paths[:out], "loading...done")
      output.skip(:out => 10, :err => 0)

      expect(read).to eq([[:out, "done"]])
    end

    it "reads a flood of output in chunks" do
      stub_const("RSpec::Core::Parallel::WorkerOutput::CHUNK", 4)
      File.write(output.paths[:out], "0123456789")

      expect(read.map(&:last)).to eq(%w[0123 4567 89])
    end

    it "yields nothing for files that are gone" do
      output.paths.each_value { |path| File.delete(path) }
      expect(read).to eq([])
    end

    it "goes without a directory when none can be made" do
      allow(Dir).to receive(:mktmpdir).and_raise(Errno::EACCES)
      expect(WorkerOutput.make_directory).to be_nil
      expect { WorkerOutput.remove_directory(nil) }.not_to raise_error
    end

    describe WorkerOutput::Capture do
      let(:results) { Channel.pipe }
      let(:stdout) { File.open(File.join(dir, 'stdout'), 'w') }
      let(:stderr) { File.open(File.join(dir, 'stderr'), 'w') }

      before do
        stub_const('STDOUT', stdout)
        stub_const('STDERR', stderr)
      end

      after do
        results.each(&:close)
        [stdout, stderr].each(&:close)
      end

      it "moves standard output and error into the files and says how far they reach before a frame that follows output" do
        capture = WorkerOutput::Capture.new(output.paths, results.last)
        STDOUT.write("printed")
        STDERR.write("warned")

        capture.send([:first])
        capture.send([:second])

        frames = results.first.drain.map(&:first)
        expect(frames).to eq([[:output, { :out => 7, :err => 6 }], [:first], [:second]])
        expect(read(frames.first.last)).to eq([[:out, "printed"], [:err, "warned"]])
        expect([STDOUT.sync, STDERR.sync]).to eq([true, true])
      end
    end
  end
end
