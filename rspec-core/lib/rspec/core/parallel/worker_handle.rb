module RSpec
  module Core
    module Parallel
      # Coordinator-side bookkeeping for one worker process.
      # @private
      class WorkerHandle
        attr_reader :number, :jobs, :results, :child_jobs, :child_results, :current_example, :current_example_started_at
        attr_accessor :pid, :current_unit, :state, :deadline, :escalation, :exit_status, :quit_sent, :exiting
        # Forked before the spec files were loaded and has not yet reported
        # what it loaded.
        attr_accessor :loading

        def initialize(number)
          @number = number
          @child_jobs, @jobs = Channel.pipe
          @results, @child_results = Channel.pipe
          @state = :running
          @quit_sent = false
          @exiting = false
          @loading = false
          @pid = @current_unit = @deadline = @escalation = @exit_status = nil
          @current_example = @current_example_started_at = nil
        end

        def current_example=(example)
          @current_example = example
          @current_example_started_at = example && RSpec::Core::Time.now
        end

        # @return [String, nil] the example this worker is in the middle of,
        #   and for how long it has been running
        def running_example_description
          return nil unless @current_example

          "#{description} was running #{@current_example.location_rerun_argument || @current_example.id} " \
            "(#{@current_example.full_description}) for " \
            "#{Parallel.seconds(RSpec::Core::Time.now - @current_example_started_at)}."
        end

        # @return [Array(Example, Hash)] the example this worker was running
        #   when it was lost, and a result failing it with `text`
        def lost_example_result(text)
          example = @current_example
          exception = WorkerError.new(text)
          exception.set_backtrace([example.location])
          finished_at = RSpec::Core::Time.now
          result = { :status => :failed, :started_at => @current_example_started_at, :finished_at => finished_at,
                     :run_time => finished_at - @current_example_started_at,
                     :exception => RemoteException.payload_for(exception) }
          self.current_example = nil
          [example, result]
        end

        # @return [Integer] the pid of the worker process forked to run `units`,
        #   or to load the spec files itself when there are none yet
        def fork_worker(process, units, handles, worker_options={})
          process.fork do
            # :nocov: - runs in the forked worker process, covered by integration specs
            handles.each { |other| other.close_all unless other.equal?(self) }
            close_parent_ends
            Worker.new(@number, units, @child_jobs, @child_results, worker_options).run
            # :nocov:
          end
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
          @state == :running && @current_unit.nil? && !@loading
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

        # Sends `signal_name` as the `escalation` step that is followed up at
        # `deadline`.
        def signal(process, signal_name, escalation, deadline)
          @escalation = escalation
          @deadline = deadline
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
