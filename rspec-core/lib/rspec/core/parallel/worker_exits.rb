module RSpec
  module Core
    module Parallel
      # The part of the {Coordinator} that deals with a worker that leaves
      # before its work is done: lost -- it crashed, was killed or had to be
      # terminated -- when the example it was running fails and what it had
      # not started goes to another worker, or ending the run because the
      # code it ran called `exit`, as a serial run would end.
      # @private
      module WorkerExits
        SENT_TERM = ', so it was sent SIGTERM'
        RERUN_ARGUMENTS_SHOWN = 50
        RERUN_IDS_PER_UNIT = 20

      private

        def judge_exit(worker, status)
          case worker.state
          when :running
            # Asked to stop early, a worker may leave as soon as its group ends.
            return if status&.success? && worker.exiting && worker.current_unit.nil?
            return left_by_exit(worker, status) if worker.called_exit

            lost(worker, "exited unexpectedly (#{WorkerHandle.describe_status(status)})")
          when :stopping
            return left_by_exit(worker, status) if worker.called_exit
            return if status&.success?

            error("#{worker.description} failed while shutting down (#{WorkerHandle.describe_status(status)}). " \
                  "If its exit watchdog fired, its thread dump is on stderr above.")
          end
        end

        def overdue(worker)
          timeout = Parallel.seconds(@configuration.parallel_shutdown_timeout)
          if worker.called_exit
            @exit_spoiled = true
            error("#{worker.description} did not exit within #{timeout} of ending the run because " \
                  "#{worker.exit_cause} (its `after(:suite)` hooks or `at_exit` handlers may be stuck), so it was " \
                  "sent SIGTERM. Raise `config.parallel_shutdown_timeout` if they legitimately need longer.")
            abort_unit(worker)
            worker.results.close
          elsif worker.state == :stopping
            error("#{worker.description} did not exit within #{timeout} of finishing its last example group (its " \
                  "`after(:suite)` hooks or `at_exit` handlers may be stuck), so it was sent SIGTERM. Raise " \
                  "`config.parallel_shutdown_timeout` if it legitimately needs longer.")
          elsif worker.loading
            lost(worker, "took longer than #{Parallel.seconds(@load_allowance)}", SENT_TERM)
          elsif worker.quit_sent
            lost(worker, "did not stop within #{timeout} of being asked to stop early (fail-fast or interrupt)", SENT_TERM)
          elsif worker.exiting
            lost(worker, "did not exit within #{timeout} of leaving its job loop (its `after(:suite)` hooks or " \
                         "`at_exit` handlers may be stuck; raise `config.parallel_shutdown_timeout` if they " \
                         "legitimately need longer)", SENT_TERM)
          else
            lost(worker, "stopped reporting for #{timeout}", SENT_TERM)
          end
        end

        def terminate(worker, reason)
          lost(worker, reason, SENT_TERM)
          worker.state = :terminating
          worker.signal(@process, 'TERM', :term, now + @term_grace)
        end

        # The worker has left its job loop and runs its `after(:suite)` hooks
        # and `at_exit` handlers: as long as that may take is bounded here
        # too, also for a worker that left in the middle of a group -- from
        # now, so that its own watchdog, which it armed on leaving, fires
        # first, also when it was asked to stop earlier.
        def exiting(worker, cause, exit_status=nil)
          worker.exiting = true
          worker.exit_cause ||= cause
          teardown = now + @configuration.parallel_shutdown_timeout + @watchdog_grace
          worker.deadline = teardown unless worker.escalation || (worker.deadline && worker.deadline >= teardown)
          ended_by_exit(worker, exit_status) if exit_status
        end

        # RSpec does not rescue `SystemExit`, so in a serial run `exit` ends
        # the run: here too no example starts from now on, busy workers stop
        # after the one they are running and the run exits with that status
        # -- the first one, if more workers call `exit`.
        def ended_by_exit(worker, exit_status)
          worker.called_exit = exit_status
          # Leaving anyway: it is not asked to stop.
          worker.quit_sent = true
          return if @exit_status

          @exit_status = exit_status
          running = worker.current_example
          report "#{worker.description} is ending the run: #{worker.exit_cause}" \
                 "#{" in #{running.full_description} (#{running.location})" if running}. As in a serial run, no " \
                 "further examples start and the run exits with status #{exit_status}."
          request_quit
        end

        # The worker can no longer be relied on, and nothing it sends from now
        # on is read: it would report the example a second time. What it was
        # running is reported as lost -- in a way that the summary, `Failed
        # examples:` and `.rspec_status` point at -- and what it had not
        # started of its unit goes to another worker.
        def lost(worker, what, consequence='')
          because = "#{consequence}#{worker.exit_cause && ": #{worker.exit_cause}"}"
          if worker.loading
            worker.loading = false
            error("#{worker.description} #{what} while loading the spec files#{consequence}.")
          elsif worker.current_example
            lost_in_example(worker, "#{worker.description} #{what} while running this example#{because}")
          elsif worker.current_unit
            lost_in_unit(worker, "#{worker.description} #{what}", because)
          else
            error("#{worker.description} #{what} between example groups#{because}.")
          end
          abort_unit(worker)
          worker.results.close
        end

        # Its example fails and never runs again -- what it did may be why the
        # worker was lost.
        def lost_in_example(worker, reason)
          unit = @units[worker.current_unit]
          rest = unit.ids.reject { |id| worker.started_example_ids.key?(id) }
          requeue = !rest.empty? && accepting_work? && !fail_fast_limit_met?(@results.failed_count + 1)
          example, result = worker.lost_example_result("#{reason}#{rest_clause(unit, rest, requeue)}.")
          record_result(example, result)
          @sequencer.example_finished(worker.number, example, 0)
          queue_first(Unit.new(unit.group, rest)) if requeue
        end

        # Lost in the `before(:context)` or `after(:context)` hooks of a group
        # or between its examples: the examples of the group it started last
        # that had not started fail, as they do when such a hook raises, and
        # the rest goes to another worker. (Its groups may have reported
        # finishing as the process unwound.) Lost before it even started the
        # unit's group, it is given one more go.
        def lost_in_unit(worker, who, because)
          unit = @units[worker.current_unit]
          unstarted = unit.ids.reject { |id| worker.started_example_ids.key?(id) }
          inside = worker.last_started_group
          if inside
            within = inside.descendant_filtered_examples.each_with_object({}) { |example, ids| ids[example.id] = true }
            failing, rest = unstarted.partition { |id| within.key?(id) }
            where = "while running #{inside.metadata[:full_description]} (#{inside.location}), outside of any example"
          else
            retry_it = worker.started_example_ids.empty? && !unit.retried
            failing, rest = retry_it ? [[], unstarted] : [unstarted, []]
            where = "before it started #{unit.description}"
          end

          requeue = !rest.empty? && accepting_work?
          if failing.empty?
            error("#{who} #{where}#{because}#{rest_clause(unit, rest, requeue)}.")
          else
            fail_unstarted(worker, failing, "#{who} #{where}#{because}, so this example could not run" \
                                            "#{rest_clause(unit, rest, requeue)}.")
          end
          queue_first(Unit.new(unit.group, rest, !inside)) if requeue
        end

        def fail_unstarted(worker, ids, reason)
          ids.each do |id|
            index.with_example(id) do |example|
              @sequencer.example_started(worker.number, example, 0)
              record_result(example, WorkerHandle.failed_result(example, reason))
              @sequencer.example_finished(worker.number, example, 0)
            end
          end
        end

        def rest_clause(unit, rest, requeued)
          return "" if rest.empty?

          examples = pluralize(rest.size, 'example')
          if requeued
            "; #{examples} of #{unit.description} that it had not started yet will run in another worker"
          else
            "; #{examples} of #{unit.description} that it had not started did not run"
          end
        end

        def accepting_work?
          !@quitting && !@winding_down
        end

        def queue_first(unit)
          @units << unit
          @unit_states << :pending
          @queue.unshift(@units.size - 1)
        end

        # A worker that called `exit` left the way a serial run does: its
        # example is neither passed nor failed. One that then exited
        # otherwise was aborted by its watchdog or killed.
        def left_by_exit(worker, status)
          unless status.nil? || (status.exited? && status.exitstatus == worker.called_exit)
            @exit_spoiled = true
            ended = "#{status.signaled? ? 'was' : 'exited with'} #{WorkerHandle.describe_status(status)}"
            error("#{worker.description} ended the run because #{worker.exit_cause}, then #{ended}: if its " \
                  "watchdog aborted it, its `after(:suite)` hooks or " \
                  "`at_exit` handlers did not finish within #{Parallel.seconds(@configuration.parallel_shutdown_timeout)} " \
                  "(its thread dump is on stderr above).")
          end
          abort_unit(worker)
          worker.results.close
        end

        def report_unrun_units
          return if @quitting || @queue.empty?

          units = @queue.map { |index| @units[index] }
          examples = units.sum { |unit| unit.ids.size }
          groups = units.map(&:group).uniq.size
          report "#{pluralize(examples, 'example')} of #{pluralize(groups, 'example group')} did not run because no " \
                 "worker remained to run them. To run them: rspec #{rerun_arguments(units)}"
        end

        # Quoted as RSpec quotes the examples to rerun after a `--bisect`; a
        # long list of examples of one group is given as the whole group.
        def rerun_arguments(units)
          ids = units.flat_map do |unit|
            unit.example_ids && unit.example_ids.size <= RERUN_IDS_PER_UNIT ? unit.example_ids : [unit.group.id]
          end
          arguments = Formatters::Helpers.organize_ids(ids)
          hidden = arguments.size - RERUN_ARGUMENTS_SHOWN
          shown = arguments.first(RERUN_ARGUMENTS_SHOWN).join(' ')
          hidden > 0 ? "#{shown} (and #{pluralize(hidden, 'more spec file')})" : shown
        end
      end
    end
  end
end
