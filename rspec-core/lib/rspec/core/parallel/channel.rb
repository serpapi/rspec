module RSpec
  module Core
    module Parallel
      # Length-prefixed `Marshal` frames over an anonymous pipe, exchanged only
      # between the coordinating process and the worker processes it forked
      # (the trust boundary `Bisect::Channel` already relies on). The reader
      # side never blocks the caller unless asked to: frames are reassembled
      # from whatever bytes are available, so a worker stalled mid-frame cannot
      # stall the coordinator.
      # @private
      module Channel
        ENCODING = Marshal.dump("").encoding
        CHUNK_SIZE = 65_536

        # Raised when the peer wrote something that is not a frame.
        class ProtocolError < StandardError; end

        # @return [Array(Reader, Writer)] the two ends of a new pipe
        def self.pipe
          read_io, write_io = IO.pipe
          [Reader.new(read_io), Writer.new(write_io)]
        end

        # @private
        class Writer
          attr_reader :io

          def initialize(io)
            @io = io
            @io.set_encoding ENCODING
            @io.sync = true
          end

          def send(message)
            packet = Marshal.dump(message)
            @io.write("#{packet.bytesize}\n#{packet}")
          end

          def closed?
            @io.closed?
          end

          def close
            @io.close unless @io.closed?
          end
        end

        # @private
        class Reader
          attr_reader :io, :bytes_read

          def initialize(io)
            @io = io
            @io.set_encoding ENCODING
            @buffer = String.new(:encoding => ENCODING)
            @pending = []
            @eof = false
            @bytes_read = 0
          end

          def eof?
            @eof
          end

          def closed?
            @io.closed?
          end

          def close
            @io.close unless @io.closed?
          end

          # Reads what is available without blocking.
          # @return [Array<Array(Object, Integer)>] decoded messages with their frame size in bytes
          def drain
            read_available
            messages = []
            while (frame = next_frame)
              messages << frame
            end
            messages
          end

          # Puts a message obtained from {#drain} back so that {#receive}
          # returns it first.
          def unread(message)
            @pending << message
          end

          # Blocks until a whole message has arrived.
          # @return [Object, nil] the message, or `nil` once the writer is gone
          def receive
            return @pending.shift unless @pending.empty?

            loop do
              frame = next_frame
              return frame.first if frame
              return nil if @eof

              # `IO#wait_readable` would need `io/wait`, which rspec-core does not load.
              IO.select([@io]) # rubocop:disable Lint/IncompatibleIoSelectWithFiberScheduler
              read_available
            end
          end

        private

          # Reads what the kernel has buffered, a bounded number of chunks at a
          # time so that one chatty worker cannot monopolize the coordinator.
          def read_available
            16.times do
              break if @eof

              chunk = @io.read_nonblock(CHUNK_SIZE, :exception => false)
              case chunk
              when nil then @eof = true
              when :wait_readable then break
              else
                @bytes_read += chunk.bytesize
                @buffer << chunk
              end
            end
          end

          # rubocop:disable-next Security/MarshalLoad
          def next_frame
            newline = @buffer.index("\n")
            return nil unless newline

            size = frame_size(@buffer.byteslice(0, newline))
            frame_end = newline + 1 + size
            return nil if @buffer.bytesize < frame_end

            packet = @buffer.byteslice(newline + 1, size)
            @buffer = @buffer.byteslice(frame_end, @buffer.bytesize - frame_end)
            [Marshal.load(packet), frame_end]
          end

          def frame_size(header)
            Integer(header, 10)
          rescue ArgumentError, TypeError
            raise ProtocolError, "Expected a frame length, got #{header.inspect}"
          end
        end
      end
    end
  end
end
