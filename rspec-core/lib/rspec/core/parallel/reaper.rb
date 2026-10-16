module RSpec
  module Core
    module Parallel
      # A small process that outlives the coordinator, which it watches: if
      # the coordinator is killed (SIGKILL, the OOM killer) while a worker
      # cannot notice by itself -- one stuck before running any of RSpec's
      # code, as a deadlock in a `fork` handler leaves it, or stopped -- it
      # sends the workers it was told about SIGTERM, then SIGKILL, and
      # removes their output files. It is started before the first worker is
      # forked, by `spawn` (no fork handlers run), and only where workers are
      # forked: spawned workers watch the coordinator's pid themselves. A
      # worker it was not told about yet -- the coordinator was killed
      # between forking it and telling the reaper -- is not covered.
      # @private
      class Reaper
        POLL = 0.25
        # Workers that can leave by themselves do, within their own poll.
        OWN_EXIT_GRACE = 3 * Worker::COORDINATOR_POLL
        TERM_GRACE = 5.0

        # Ignores the signals that a terminal or a job's timeout sends to
        # the whole process group, which would end it with the coordinator.
        # Polls its parent; when that changes, gives the workers still
        # registered whose process group is the run's a moment to leave by
        # themselves, signals those that did not, then exits.
        SCRIPT = <<-'RUBY'.freeze
          %w[INT TERM HUP QUIT].each { |signal| trap(signal, 'IGNORE') if Signal.list.key?(signal) }
          coordinator, group, output_dir = ARGV[0].to_i, ARGV[1].to_i, ARGV[2]
          poll, own_exit, grace = ARGV[3].to_f, ARGV[4].to_f, ARGV[5].to_f
          workers = {}
          buffer = +''
          loop do
            if IO.select([STDIN], nil, nil, poll)
              chunk = (STDIN.read_nonblock(4096) rescue nil)
              break if chunk.nil?
              buffer << chunk
              while (line = buffer.slice!(/\A[^\n]*\n/))
                exit!(0) if line == "done\n"
                pid = line[1..-1].to_i
                line.start_with?('+') ? workers[pid] = true : workers.delete(pid)
              end
            end
            break if Process.ppid != coordinator
          end
          exit!(0) if Process.ppid == coordinator
          ours = lambda do |pid|
            Process.getpgid(pid) == group
          rescue SystemCallError
            false
          end
          waiting = workers.keys.select(&ours)
          until_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) + own_exit
          sleep 0.05 while waiting.any?(&ours) && Process.clock_gettime(Process::CLOCK_MONOTONIC) < until_time
          left = waiting.select(&ours)
          left.each { |pid| Process.kill('TERM', pid) rescue nil }
          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + grace
          sleep 0.05 while left.any?(&ours) && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
          left.select(&ours).each { |pid| Process.kill('KILL', pid) rescue nil }
          unless left.empty?
            STDERR.puts "RSpec's parallel run (pid #{coordinator}) ended without stopping its workers; stopped " \
                        "#{left.size} still running (pids #{left.join(', ')})."
          end
          if output_dir && !output_dir.empty?
            require 'fileutils'
            FileUtils.remove_entry(output_dir, true)
          end
          exit!(0)
        RUBY

        # @return [Reaper, nil] one watching this process, or `nil` when it
        #   could not be started -- the run goes on without it
        def self.start(output_dir, process=Process)
          read, write = IO.pipe
          pid = process.spawn(RbConfig.ruby, '--disable-gems', '-e', SCRIPT, Process.pid.to_s, Process.getpgrp.to_s,
                              output_dir.to_s, POLL.to_s, OWN_EXIT_GRACE.to_s, TERM_GRACE.to_s,
                              :in => read, :out => File::NULL, :close_others => true)
          new(pid, write, process)
        rescue SystemCallError, NotImplementedError
          write&.close
          nil
        ensure
          read&.close
        end

        attr_reader :pid

        def initialize(pid, input, process=Process)
          @pid = pid
          @input = input
          @input.sync = true
          @process = process
        end

        def watch(pid)
          tell("+#{pid}")
        end

        def forget(pid)
          tell("-#{pid}")
        end

        # Ends it, and reaps it if it has gone within a second.
        def finish
          tell('done')
          @input.close
          deadline = Parallel.monotonic_time + 1.0
          sleep 0.01 until @process.waitpid(@pid, Process::WNOHANG) || Parallel.monotonic_time > deadline
        rescue SystemCallError
          nil
        end

      private

        def tell(line)
          @input.write("#{line}\n") unless @input.closed?
        rescue SystemCallError, IOError
          nil # it is gone; the run goes on without it
        end
      end
    end
  end
end
