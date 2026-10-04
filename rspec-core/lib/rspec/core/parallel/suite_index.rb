module RSpec
  module Core
    module Parallel
      # Finds the examples and example groups of the loaded suite by the ids
      # workers report them under.
      # @private
      class SuiteIndex
        def initialize(world)
          @world = world
        end

        def example(id)
          examples_by_id[id]
        end

        def group(id)
          groups_by_id[id]
        end

      private

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
