module RSpec::Core::Parallel
  RSpec.describe Channel do
    let(:ends) { Channel.pipe }
    let(:reader) { ends.first }
    let(:writer) { ends.last }

    after do
      reader.close
      writer.close
    end

    it "round-trips objects as length-prefixed Marshal frames" do
      writer.send([:example_finished, "./spec/a_spec.rb[1:1]", { :status => :passed, :run_time => 0.5 }])

      messages = reader.drain
      expect(messages.size).to eq(1)
      message, bytes = messages.first
      expect(message).to eq([:example_finished, "./spec/a_spec.rb[1:1]", { :status => :passed, :run_time => 0.5 }])
      expect(bytes).to eq(reader.bytes_read)
    end

    it "carries binary data without re-encoding it" do
      writer.send("caf\xC3\xA9".b)
      writer.send("café")

      expect(reader.drain.map(&:first)).to eq(["caf\xC3\xA9".b, "café"])
    end

    it "decodes several frames that arrived at once" do
      3.times { |i| writer.send(i) }

      expect(reader.drain.map(&:first)).to eq([0, 1, 2])
    end

    it "keeps a partial frame that follows whole ones, and reports each frame's own size" do
      packets = [:first, "second" * 100, :third].map { |message| Marshal.dump(message) }
      frames = packets.map { |packet| "#{packet.bytesize}\n#{packet}" }
      writer.io.write(frames[0] + frames[1] + frames[2][0, 5])

      expect(reader.drain).to eq([[:first, frames[0].bytesize], ["second" * 100, frames[1].bytesize]])

      writer.io.write(frames[2][5..-1])
      expect(reader.drain).to eq([[:third, frames[2].bytesize]])
      expect(reader.bytes_read).to eq(frames.sum(&:bytesize))
    end

    it "keeps a partial frame buffered without blocking until the rest arrives" do
      packet = Marshal.dump(:whole)
      writer.io.write("#{packet.bytesize}\n#{packet[0, 3]}")

      expect(reader.drain).to eq([])
      expect(reader.eof?).to be false

      writer.io.write(packet[3..-1])
      expect(reader.drain.map(&:first)).to eq([:whole])
    end

    it "returns nothing when no data is available, without blocking" do
      expect(reader.drain).to eq([])
    end

    it "reports EOF once the writer is closed and everything has been read" do
      writer.send(:last)
      writer.close

      expect(reader.drain.map(&:first)).to eq([:last])
      expect(reader.eof?).to be true
    end

    it "raises a ProtocolError when the peer writes something that is not a frame" do
      writer.io.write("garbage\n")

      expect { reader.drain }.to raise_error(Channel::ProtocolError, /Expected a frame length/)
    end

    describe "#receive" do
      it "blocks until a whole message is available" do
        thread = Thread.new do
          sleep 0.05
          writer.send(:late)
        end

        expect(reader.receive).to eq(:late)
        thread.join
      end

      it "returns nil at EOF" do
        writer.close
        expect(reader.receive).to be_nil
      end

      it "returns messages that were put back with #unread first" do
        writer.send(:drained)
        writer.send(:next)
        drained = reader.drain.map(&:first)
        expect(drained).to eq([:drained, :next])

        reader.unread(:next)
        writer.send(:after)

        expect(reader.receive).to eq(:next)
        expect(reader.receive).to eq(:after)
      end

      it "returns buffered messages before reading more" do
        writer.send(:first)
        writer.send(:second)

        expect(reader.receive).to eq(:first)
        expect(reader.receive).to eq(:second)
      end
    end

    it "closes both ends idempotently" do
      reader.close
      writer.close
      expect { reader.close }.not_to raise_error
      expect { writer.close }.not_to raise_error
      expect(reader).to be_closed
      expect(writer).to be_closed
    end
  end
end
