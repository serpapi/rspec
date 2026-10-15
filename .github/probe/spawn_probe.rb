# Mechanics a spawn-based worker pool for `rspec --parallel` would rely on,
# checked on the platform it runs on: pipes as a spawned child's stdin and
# stdout carrying binary Marshal frames, IO.select and read_nonblock on them,
# the child's own output moved to capture files, exit statuses, TERM/KILL,
# handle inheritance between children, and how a child notices its parent died.
require 'rbconfig'
require 'tmpdir'

$results = []
def check(name, ok, detail = nil)
  $results << ok
  puts "#{ok ? 'PASS' : 'FAIL'} #{name}#{" -- #{detail}" if detail}"
  $stdout.flush
end

def note(name, detail)
  puts "INFO #{name} -- #{detail}"
  $stdout.flush
end

def frame(obj)
  data = Marshal.dump(obj)
  [data.bytesize].pack('N') + data
end

class Reader
  attr_reader :io, :errors
  def initialize(io)
    @io = io
    @io.binmode
    @buffer = ''.b
    @eof = false
    @errors = []
  end

  def eof?
    @eof
  end

  def drain
    loop do
      chunk = begin
        @io.read_nonblock(65_536, :exception => false)
      rescue StandardError => e
        @errors << e.class.name
        nil
      end
      break if chunk == :wait_readable
      if chunk.nil?
        @eof = true
        break
      end
      @buffer << chunk
    end
    frames = []
    while @buffer.bytesize >= 4
      size = @buffer.byteslice(0, 4).unpack1('N')
      break if @buffer.bytesize < 4 + size
      frames << Marshal.load(@buffer.byteslice(4, size))
      @buffer = @buffer.byteslice(4 + size, @buffer.bytesize)
    end
    frames
  end

  def wait_for(timeout = 20)
    deadline = Time.now + timeout
    frames = []
    while frames.empty? && Time.now < deadline && !@eof
      ready, = IO.select([@io], nil, nil, 0.5)
      frames.concat(drain) if ready
    end
    frames
  end
end

CHILD = <<'RUBY'
  def frame(obj)
    data = Marshal.dump(obj)
    [data.bytesize].pack('N') + data
  end
  jobs = STDIN.dup
  jobs.binmode
  results = STDOUT.dup
  results.binmode
  results.sync = true
  STDIN.reopen(File::NULL)
  STDOUT.reopen(ENV.fetch('PROBE_OUT'), 'ab')
  STDOUT.sync = true
  STDERR.reopen(ENV.fetch('PROBE_ERR'), 'ab')
  STDERR.sync = true
  results.write(frame([:started, Process.pid, Process.ppid, ENV['TEST_ENV_NUMBER']]))
  loop do
    header = jobs.read(4)
    exit!(9) unless header && header.bytesize == 4
    command, payload = Marshal.load(jobs.read(header.unpack1('N')))
    case command
    when :echo
      results.write(frame([:echo, payload]))
    when :print
      puts 'out-line'
      $stderr.puts 'err-line'
      print "no-newline"
      system(RbConfig.ruby, '-e', 'puts "grandchild"; $stdout.flush')
      results.write(frame([:printed, File.size(ENV['PROBE_OUT']), File.size(ENV['PROBE_ERR'])]))
    when :sleep
      results.write(frame([:sleeping]))
      sleep
    when :exit
      exit(payload)
    end
  end
RUBY

MIDDLE = <<'RUBY'
  child = Process.spawn({ 'PROBE_WATCH' => ENV.fetch('PROBE_WATCH') },
                        RbConfig.ruby, '-e', ENV.fetch('PROBE_CHILD'), :in => File::NULL, :out => File::NULL)
  File.write(ENV.fetch('PROBE_CHILD_PID'), child.to_s)
  sleep 1
  exit!(0)
RUBY

WATCHER = <<'RUBY'
  parent = Process.ppid
  deadline = Time.now + 6
  changed = gone = false
  until Time.now > deadline
    changed ||= Process.ppid != parent
    begin
      Process.kill(0, parent)
    rescue Errno::ESRCH
      gone = true
    rescue StandardError
      nil
    end
    break if changed || gone
    sleep 0.1
  end
  File.binwrite(ENV.fetch('PROBE_WATCH'), Marshal.dump([parent, Process.ppid, changed, gone, Time.now.to_f]))
RUBY

def spawn_child(dir, name, env = {})
  jobs_r, jobs_w = IO.pipe
  results_r, results_w = IO.pipe
  jobs_w.binmode
  jobs_w.sync = true
  out = File.join(dir, "#{name}.out")
  err = File.join(dir, "#{name}.err")
  File.write(out, '')
  File.write(err, '')
  started = Time.now
  pid = Process.spawn({ 'PROBE_OUT' => out, 'PROBE_ERR' => err }.merge(env),
                      RbConfig.ruby, '-e', CHILD, :in => jobs_r, :out => results_w)
  jobs_r.close
  results_w.close
  { :pid => pid, :jobs => jobs_w, :results => Reader.new(results_r), :out => out, :err => err, :spawned_in => Time.now - started }
end

def send_job(child, *message)
  child[:jobs].write(frame(message))
end

def reap(pid, timeout)
  deadline = Time.now + timeout
  until Time.now > deadline
    result = Process.wait2(pid, Process::WNOHANG)
    return result[1] if result
    sleep 0.05
  end
  nil
rescue Errno::ECHILD
  :echild
end

puts "platform: #{RUBY_ENGINE} #{RUBY_ENGINE_VERSION} (ruby #{RUBY_VERSION}) #{RUBY_PLATFORM}"
fork_works = begin
  pid = Process.fork { exit!(0) }
  Process.wait(pid)
  true
rescue NotImplementedError, NoMethodError
  false
end
puts "fork works: #{fork_works}"
puts "Signal.list has TERM: #{Signal.list.key?('TERM')}, KILL: #{Signal.list.key?('KILL')}, INT: #{Signal.list.key?('INT')}"

Dir.mktmpdir('spawn-probe') do |dir|
  a = spawn_child(dir, 'a', 'TEST_ENV_NUMBER' => '')
  b = spawn_child(dir, 'b', 'TEST_ENV_NUMBER' => '2')
  note 'spawn time', format('%.3f s, %.3f s', a[:spawned_in], b[:spawned_in])

  boot = Time.now
  fa = a[:results].wait_for(60)
  fb = b[:results].wait_for(60)
  note 'boot to first frame', format('%.3f s', Time.now - boot)
  check 'both children report :started', fa.first&.first == :started && fb.first&.first == :started, [fa, fb].inspect
  check 'distinct pids', fa.first && fb.first && fa.first[1] != fb.first[1] && fa.first[1] == a[:pid], "#{fa.first&.at(1)} #{fb.first&.at(1)} spawn=#{a[:pid]},#{b[:pid]}"
  check 'ppid is this process', fa.first && fa.first[2] == Process.pid, "child ppid=#{fa.first&.at(2)} me=#{Process.pid}"
  check 'TEST_ENV_NUMBER passed', fa.first && fa.first[3] == '' && fb.first && fb.first[3] == '2'

  payloads = { 'all bytes' => (0..255).map(&:chr).join.b, 'crlf and ctrl-z' => "a\r\nb\nc\x1ad\r".b,
               '1 MB' => ('x' * 1_048_576), 'utf-8' => "Grüße ✓" }
  payloads.each do |label, payload|
    send_job(a, :echo, payload)
    got = a[:results].wait_for(30)
    check "binary frame round trip: #{label}", got.first == [:echo, payload], got.first.inspect[0, 80]
  end

  send_job(a, :print)
  printed = a[:results].wait_for(30).first
  out = File.binread(a[:out])
  err = File.binread(a[:err])
  check 'child stdout reaches the capture file in order', out == "out-line\nno-newlinegrandchild\n" || out == "out-line\r\nno-newlinegrandchild\r\n", out.inspect
  check 'child stderr reaches the capture file', err.include?('err-line'), err.inspect
  check 'reported sizes match the files', printed && printed[1] == out.bytesize && printed[2] == err.bytesize, printed.inspect
  note 'newlines in capture', out.include?("\r\n") ? 'CRLF' : 'LF'

  send_job(b, :exit, 3)
  status = reap(b[:pid], 20)
  b[:results].wait_for(5)
  check 'exit status seen with WNOHANG', status.respond_to?(:exitstatus) && status.exitstatus == 3, status.inspect
  check 'results pipe reaches EOF after exit', b[:results].eof?, "errors=#{b[:results].errors.uniq}"
  begin
    Process.kill(0, b[:pid])
    note 'kill(0) on a reaped pid', 'no error'
  rescue StandardError => e
    note 'kill(0) on a reaped pid', e.class.name
  end

  c = spawn_child(dir, 'c')
  c[:results].wait_for(60)
  send_job(c, :sleep)
  c[:results].wait_for(10)
  term = begin
    Process.kill('TERM', c[:pid])
    'ok'
  rescue StandardError => e
    e.class.name
  end
  status = reap(c[:pid], 3)
  note 'SIGTERM to a sleeping child', "kill: #{term}; reaped within 3 s: #{status.inspect}"
  unless status
    kill = begin
      Process.kill('KILL', c[:pid])
      'ok'
    rescue StandardError => e
      e.class.name
    end
    status = reap(c[:pid], 10)
    note 'SIGKILL after TERM', "kill: #{kill}"
  end
  check 'a signalled child is reaped', !status.nil?, status.inspect

  # Handle inheritance: e and f spawned while each other's pipes exist.
  e = spawn_child(dir, 'e')
  f = spawn_child(dir, 'f')
  [e, f].each { |child| child[:results].wait_for(60) }
  send_job(e, :exit, 0)
  reap(e[:pid], 20)
  e[:results].wait_for(5)
  check 'EOF on a dead child\'s pipe although a later child is alive (no inherited write end)', e[:results].eof?
  f[:jobs].close
  status = reap(f[:pid], 20)
  check 'closing the jobs pipe ends a waiting child (it sees EOF)', status.respond_to?(:exitstatus) && status.exitstatus == 9, status.inspect

  # Parent death: middle spawns a watcher, then exits.
  watch = File.join(dir, 'watch')
  child_pid_file = File.join(dir, 'child.pid')
  middle = Process.spawn({ 'PROBE_CHILD' => WATCHER, 'PROBE_CHILD_PID' => child_pid_file, 'PROBE_WATCH' => watch },
                         RbConfig.ruby, '-e', MIDDLE)
  reap(middle, 30)
  middle_exited = Time.now.to_f
  deadline = Time.now + 15
  sleep 0.2 until (File.exist?(watch) && File.size(watch) > 0) || Time.now > deadline
  if File.exist?(watch) && File.size(watch) > 0
    parent, ppid, changed, gone, at = Marshal.load(File.binread(watch))
    note 'orphan notices parent death', "ppid changed: #{changed} (#{parent} -> #{ppid}); kill(0, parent) ESRCH: #{gone}; after #{format('%.2f', at - middle_exited)} s"
    check 'an orphan can tell its parent died (ppid or kill(0))', changed || gone
  else
    check 'an orphan can tell its parent died (ppid or kill(0))', false, 'watcher wrote nothing'
  end
  if File.exist?(child_pid_file)
    begin
      Process.kill('KILL', File.read(child_pid_file).to_i)
    rescue StandardError
      nil
    end
  end

  [a, c, e, f].each do |child|
    begin
      Process.kill('KILL', child[:pid])
    rescue StandardError
      nil
    end
  end
end

failed = $results.count(false)
puts "#{$results.size} checks, #{failed} failed"
exit(failed.zero? ? 0 : 1)
