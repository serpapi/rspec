module RSpec
  module Core
    module Parallel
      # Finds the examples and example groups of the loaded suite by the ids
      # workers report them under.
      # @private
      class SuiteIndex
        # @param on_unknown [Proc] called with a warning, once for each id the
        #   suite does not have
        def initialize(world, &on_unknown)
          @world = world
          @on_unknown = on_unknown
          @unknown_ids = []
        end

        def with_example(id, &block)
          with_known(examples_by_id[id], id, &block)
        end

        def with_group(id, &block)
          with_known(groups_by_id[id], id, &block)
        end

      private

        # Examples and groups defined while running (rather than loading) the
        # suite exist only in the worker that defined them.
        def with_known(target, id)
          return yield(target) if target
          return if @unknown_ids.include?(id)

          @unknown_ids << id
          @on_unknown.call("A parallel worker reported results for #{id}, which was not defined when the suite was " \
                           "loaded; examples and groups defined at run time cannot be reported by `--parallel`.")
        end

        def examples_by_id
          @examples_by_id ||= @world.all_examples.each_with_object({}) { |example, hash| hash[example.id] = example }
        end

        def groups_by_id
          @groups_by_id ||= @world.all_example_groups.each_with_object({}) { |group, hash| hash[group.id] = group }
        end
      end
    end
  end
end
