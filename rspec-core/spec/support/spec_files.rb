RSpec.shared_context "spec files" do
  # Written outside `spec/` so that the library-wide checks never glob them,
  # and named per parallel worker so that one worker's `after(:all)` cannot
  # delete the files another worker is loading.
  def spec_file_path(basename)
    File.expand_path("../../../tmp/#{basename}#{ENV['TEST_ENV_NUMBER']}.rb", __FILE__)
  end

  def failing_spec_filename
    @failing_spec_filename ||= spec_file_path("_failing_spec")
  end

  def passing_spec_filename
    @passing_spec_filename ||= spec_file_path("_passing_spec")
  end

  def create_passing_spec_file
    FileUtils.mkdir_p(File.dirname(passing_spec_filename))
    File.open(passing_spec_filename, 'w') do |f|
      f.write %q{
        RSpec.describe "passing spec" do
          it "passes" do
            expect(1).to eq(1)
          end
        end
      }
    end
  end

  def create_failing_spec_file
    FileUtils.mkdir_p(File.dirname(failing_spec_filename))
    File.open(failing_spec_filename, 'w') do |f|
      f.write %q{
        RSpec.describe "failing spec" do
          it "fails" do
            expect(1).to eq(2)
          end
        end
      }
    end
  end

  before(:all) do
    create_passing_spec_file
    create_failing_spec_file
  end

  after(:all) do
    File.delete(passing_spec_filename)
    File.delete(failing_spec_filename)
  end

end
