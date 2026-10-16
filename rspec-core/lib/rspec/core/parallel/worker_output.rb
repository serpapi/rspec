module RSpec
  module Core
    module Parallel
      # A worker's standard output and error -- what its examples print,
      # warnings, the output of processes they start, a crash report -- go to
      # two files that only the coordinator reads. Before each frame it sends,
      # the worker says how far the files reach, so the coordinator shows
      # what was printed in the order the worker reported its examples: with
      # the example that printed it, as in a serial run. What is still in the
      # files when the worker has gone is read then.
      # @private
      class WorkerOutput
        STREAMS = [:out, :err].freeze
        # Read at a time, so that the sequencer's byte bound applies to a
        # flood of output too.
        CHUNK = 1024 * 1024

        # @return [String, nil] a directory only this user can read, or `nil`
        #   when none can be made: workers then write where the run does
        def self.make_directory
          require 'tmpdir'
          Dir.mktmpdir('rspec-parallel-')
        rescue SystemCallError, IOError
          nil
        end

        def self.remove_directory(dir)
          return unless dir

          require 'fileutils'
          FileUtils.remove_entry(dir, true)
        end

        attr_reader :paths

        def initialize(dir, number)
          @paths = STREAMS.to_h { |stream| [stream, File.join(dir, "worker-#{number}.#{stream}")] }
          @paths.each_value { |path| File.open(path, 'wb') {} }
          @readers = {}
          @read = { :out => 0, :err => 0 }
        end

        # Yields `stream, bytes` for what the worker wrote up to `sizes`
        # (`{:out => n, :err => m}`), or to the end of the files.
        def read(sizes=nil, &block)
          STREAMS.each { |stream| read_stream(stream, sizes && sizes[stream], &block) }
        end

        # Moves on past what the worker wrote up to `sizes` without showing it.
        def skip(sizes)
          STREAMS.each { |stream| @read[stream] = [@read[stream], sizes[stream]].max }
        end

        def close
          @readers.each_value(&:close)
          @readers.clear
        end

      private

        def read_stream(stream, limit)
          reader = (@readers[stream] ||= File.open(@paths[stream], 'rb'))
          loop do
            wanted = limit ? limit - @read[stream] : CHUNK
            break if wanted <= 0

            reader.pos = @read[stream]
            bytes = reader.read([wanted, CHUNK].min)
            break if bytes.nil? || bytes.empty?

            @read[stream] += bytes.bytesize
            yield stream, bytes
          end
        rescue SystemCallError, IOError
          nil
        end

        # The worker's side: its standard output and error moved into the
        # files, and the results channel it reports through, which announces
        # new output before each frame.
        # @private
        class Capture
          def initialize(paths, results)
            @results = results
            @files = paths.transform_values { |path| File.open(path, 'ab') }
            STDOUT.reopen(@files[:out])
            STDERR.reopen(@files[:err])
            [STDOUT, STDERR].each do |io|
              io.binmode
              io.sync = true
            end
            @announced = { :out => 0, :err => 0 }
          end

          def send(message)
            sizes = @files.transform_values(&:size)
            unless sizes == @announced
              @results.send([:output, sizes])
              @announced = sizes
            end
            @results.send(message)
          end
        end
      end
    end
  end
end
