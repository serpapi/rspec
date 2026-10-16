require 'rspec/core/parallel/coordinator'

module RSpec::Core::Parallel
  RSpec.describe Reaper, :skip => !RSpec::Support::RubyFeatures.fork_supported? do
    def alive?(pid)
      state = `ps -o stat= -p #{pid}`.strip
      !state.empty? && !state.start_with?('Z')
    end

    def wait_until(seconds)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
      sleep 0.05 until yield || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      yield
    end

    it "ends when told the run is done, without signalling anything" do
      reaper = Reaper.start(nil)
      sleeper = Process.spawn('sleep', '30')
      reaper.watch(sleeper)

      reaper.finish

      expect { Process.kill(0, reaper.pid) }.to raise_error(Errno::ESRCH)
      expect(alive?(sleeper)).to be true
    ensure
      Process.kill('KILL', sleeper) && Process.wait(sleeper) if sleeper
    end

    it "stops only the registered processes of the run's process group once its parent is gone without saying so" do
      script = <<-RUBY
        #{$LOAD_PATH.map { |path| "$LOAD_PATH.unshift(#{path.to_s.inspect})" }.join("\n")}
        require 'rspec/core'
        require 'rspec/core/parallel/coordinator'
        registered = Process.spawn('sleep', '30', :out => File::NULL)
        elsewhere = Process.spawn('sleep', '30', :out => File::NULL, :pgroup => true)
        unregistered = Process.spawn('sleep', '30', :out => File::NULL)
        reaper = RSpec::Core::Parallel::Reaper.start(nil)
        reaper.watch(registered)
        reaper.watch(elsewhere)
        puts [registered, elsewhere, unregistered].join(' ')
        STDOUT.flush
        exit!(0)
      RUBY
      registered, elsewhere, unregistered = IO.popen([RbConfig.ruby, '-e', script], :err => File::NULL, &:read).split.map(&:to_i)

      expect(wait_until(15) { !alive?(registered) }).to be true
      expect([alive?(elsewhere), alive?(unregistered)]).to eq([true, true])
    ensure
      [registered, elsewhere, unregistered].compact.each do |pid|
        Process.kill('KILL', pid) if alive?(pid)
      rescue Errno::ESRCH
        nil
      end
    end

    it "is not started when the system refuses it a process, and the run goes on" do
      process = double(:process)
      allow(process).to receive(:spawn).and_raise(Errno::EAGAIN)

      expect(Reaper.start(nil, process)).to be_nil
    end

    it "is told nothing more once it is gone" do
      reaper = Reaper.start(nil)
      Process.kill('KILL', reaper.pid)
      Process.wait(reaper.pid)

      expect { 100.times { reaper.watch(1) } }.not_to raise_error
      expect { reaper.finish }.not_to raise_error
    end
  end
end
