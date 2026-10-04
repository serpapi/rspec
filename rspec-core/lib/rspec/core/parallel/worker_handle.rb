module RSpec
  module Core
    module Parallel
      # Coordinator-side bookkeeping for one worker process.
      # @private
      class WorkerHandle
        attr_reader :number, :jobs, :results, :child_jobs, :child_results
        attr_accessor :pid, :current_unit, :state, :deadline, :escalation, :exit_status, :quit_sent, :exiting

        def initialize(number)
          @number = number
          @child_jobs, @jobs = Channel.pipe
          @results, @child_results = Channel.pipe
          @state = :running
          @quit_sent = false
          @exiting = false
        end

        def close_parent_ends
          @jobs.close
          @results.close
        end

        def close_child_ends
          @child_jobs.close
          @child_results.close
        end

        def close_all
          close_parent_ends
          close_child_ends
        end

        def idle?
          @state == :running && @current_unit.nil?
        end

        def busy?
          @state == :running && !@current_unit.nil?
        end

        def done?
          @state == :exited || @state == :abandoned
        end

        def description
          "Parallel worker #{@number} (pid #{@pid})"
        end

        # @return [Array(Integer, Process::Status), nil] like `waitpid2` with
        #   `WNOHANG`; a process reaped by someone else counts as exited
        def reap(process)
          process.waitpid2(@pid, Process::WNOHANG)
        rescue Errno::ECHILD
          [@pid, nil]
        end

        def kill(process, signal_name)
          process.kill(signal_name, @pid) if @pid
        rescue Errno::ESRCH, Errno::EPERM
          nil
        end

        def self.describe_status(status)
          return "already reaped" unless status
          return "killed by SIG#{Signal.signame(status.termsig)}" if status.signaled?

          "exit status #{status.exitstatus}"
        end
      end
    end
  end
end
