unless defined? $__rq_jobrunnerdaemon__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'drb/drb'
    require 'fileutils'
    require 'tmpdir'
    require 'tempfile'

    require LIBDIR + 'job'
    require LIBDIR + 'jobrunner'

    #
    # as stated in the description of the JobRunner class, the JobRunnerDaemon
    # is a helper daemon that runs as a drb object.  it's primary responsibilty
    # is simply for enable forks to occur in a a different address space that
    # the one doing the sqlite transaction.  in addition to forking to create
    # child processes in which to run jobs, the JobRunnerDaemon daemon also
    # provides facilities to wait for these children
    #
    class  JobRunnerDaemon
#--{{{
      include Logging

      class << self
#--{{{
        def daemon(*a,&b)
#--{{{
          jrd = new(*a, &b) 

          r, w = IO::pipe

          unless((pid = fork)) # child
            $0 = "#{ self }".gsub(%r/[^a-zA-Z]+/,'_').downcase
            begin
              r.close
              n = 0
              uri = nil
              socket = nil

              42.times do
                begin
                  s = "%s/%s_%s_%s_%s" %
                    [Dir::tmpdir, File::basename($0), Process::ppid, n, rand(42)]
                  u = "drbunix://#{ s }"
                  DRb::start_service u, jrd 
                  socket = s
                  uri = u
                  break
                rescue Errno::EADDRINUSE
                  n += 1
                end
              end

              if socket and uri
                w.write socket 
                w.close
                pid = Process::pid
                ppid = Process::ppid
                cur = Thread::current
                Thread::new(pid, ppid, cur) do |pid, ppid, cur|
                  loop do
                    begin
                      Process::kill 0, ppid
                      sleep 42
                    rescue
                      cur.raise "parent <#{ ppid }> died unexpectedly" 
                    end
                  end
                end
                DRb::thread.join
              else
                w.close
              end
            ensure
              exit!
            end
          else # parent
            w.close
            socket = r.read
            r.close

            if socket and File::exist?(socket)
              at_exit{ FileUtils::rm_f socket }
              uri = "drbunix://#{ socket }"
            #
            # starting this on localhost avoids dns lookups!
            #
              DRb::start_service 'druby://localhost:0', nil
              jrd = DRbObject::new nil, uri
              jrd.pid = pid
              jrd.uri = uri
            else
              raise "failed to start job runner daemon"
            end
          end

          return jrd
#--}}}
        end
#--}}}
      end
      attr :q
      attr :runners
      attr :pid, true
      attr :uri, true
      def initialize q
#--{{{
        @q = q
        @runners = {}
        @uri = nil
        @pid = Process::pid 
#--}}}
      end
      def runner job 
#--{{{
        r = nil
        retried = false
        begin
          r = JobRunner::new @q, job
        rescue Errno::ENOMEM, Errno::EAGAIN
          GC::start
          unless retried
            retried = true 
            retry
          else
            raise
          end
        end
        @runners[r.pid] = r
        r
#--}}}
      end
      def wait
#--{{{
        pid = Process::wait
        @runners.delete pid
        pid
#--}}}
      end
      def wait2
#--{{{
        pid, status = Process::wait2
        @runners.delete pid
        [pid, status]
#--}}}
      end
      def waitpid pid = -1, flags = 0 
#--{{{
        pid = pid.pid if pid.respond_to? 'pid'
        pid = Process::waitpid pid, flags 
        @runners.delete pid
        pid
#--}}}
      end
      def waitpid2 pid = -1, flags = 0 
#--{{{
        pid = pid.pid if pid.respond_to? 'pid'
        pid, status = Process::waitpid2 pid, flags 
        @runners.delete pid
        [pid, status]
#--}}}
      end
      def shutdown
#--{{{
        @death =
          Thread::new do
            begin
              while not @runners.empty?
                pid = Process::wait 
                @runners.delete pid
              end
            ensure
              #sleep 4.2
              DRb::thread.kill
              Thread::main exit!
            end
          end
#--}}}
      end
      def install_signal_handlers
#--{{{
        %w(TERM INT HUP).each{|sig| trap sig, 'SIG_IGN'}
#--}}}
      end
#--}}}
    end # class JobRunnerDaemon
#--}}}
  end # module RQ
$__rq_jobrunnerdaemon__ = __FILE__ 
end
