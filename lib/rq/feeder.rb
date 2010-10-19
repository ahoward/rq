unless defined? $__rq_feeder__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'fileutils'

    require LIBDIR + 'mainhelper'
    require LIBDIR + 'job'
    require LIBDIR + 'jobrunner'
    require LIBDIR + 'jobrunnerdaemon'
    require LIBDIR + 'jobqueue'


#
# TODO - resolve elapsed time bug with throttle/sleep
#

    #
    # the Feeder class is responsible for running jobs from a queue - or
    # 'feeding' from that queue.  the mode of operation is essentially to run
    # jobs as quickly as possible, return them to the queue, and then to run
    # more jobs if any exist.  if no jobs exist the Feeder will periodically
    # poll the queue to see if any new jobs have arrived.
    #
    class  Feeder < MainHelper
#--{{{
      DEFAULT_MIN_SLEEP = 42
      DEFAULT_MAX_SLEEP = 240
      DEFAULT_FEED      = 2

      class << self
#--{{{
        attr :min_sleep, true
        attr :max_sleep, true
        attr :feed, true
#--}}}
      end

      def feed
#--{{{
        daemon do
          gen_pidfile
          @main.init_logging
          @logger = @main.logger
          set_q

          @pid = Process::pid
          @cmd = @main.cmd 
          @started = Util::timestamp
          @min_sleep = Integer(@options['min_sleep'] || defval('min_sleep'))
          @max_sleep = Integer(@options['max_sleep'] || defval('max_sleep'))
          @max_feed = Integer(@options['max_feed'] || defval('feed'))
          @loops = Integer @options['loops'] rescue nil
          @children = Hash::new 
          @jrd = JobRunnerDaemon::daemon @q

          install_signal_handlers

          if @daemon and not @quiet
            STDOUT.puts "pid <#{ Process::pid }> started"
            STDOUT.flush
          end

          install_redirects

          info{ "** STARTED **" }
          info{ "version <#{ RQ::VERSION }>" }
          info{ "cmd <#{ @cmd }>" }
          info{ "pid <#{ @pid }>" }
          info{ "pidfile <#{ @pidfile.path }>" }
          info{ "jobrunnerdaemon uri <#{ @jrd.uri }> pid <#{ @jrd.pid }>" }
          info{ "qpath <#{ @qpath }>" }
          debug{ "mode <#{ @mode }>" }
          debug{ "max_feed <#{ @max_feed }>" }
          debug{ "min_sleep <#{ @min_sleep }>" }
          debug{ "max_sleep <#{ @max_sleep }>" }

          transaction do
            fill_morgue
            reap_zombie_ios
          end

          looping do
            handle_signal if $rq_signaled
            throttle(@min_sleep) do
              start_jobs unless busy?
              if nothing_running?
                relax
              else
                reap_jobs
              end
            end
          end
        end
#--}}}
      end
      def looping
  #--{{{
        @loops && @loops > 0 ? @loops.times{ yield } : loop{ yield }
  #--}}}
      end
      def daemon
#--{{{
        if @options['daemon']
          fork do
            Process::setsid
            pid =
              fork do
                Dir::chdir(Util.realpath('~'))
                File::umask 0
                @daemon = true
                yield
                exit EXIT_SUCCESS
              end
            # STDOUT.puts "#{ pid }"
            exit!
          end
          exit!
        else
          @daemon = false 
          yield
          exit EXIT_SUCCESS
        end
#--}}}
      end
      def gen_pidfile name = nil
#--{{{
        gen_pidfilepath

        begin
          FileUtils::mkdir_p(File::dirname(@pidfilepath))
        rescue
          nil
        end

        locked = nil 
        no_other_feeder = nil 

        2.times do
          locked = false
          no_other_feeder = false 

          @pidfile = 
            begin
              open @pidfilepath, File::CREAT | File::EXCL | File::RDWR
            rescue
              open @pidfilepath, File::RDWR
            end

          ret = @pidfile.posixlock(File::LOCK_EX | File::LOCK_NB)
          locked = (ret == 0)

          begin
            pid = Integer(IO::read(@pidfilepath)) rescue nil

            unless pid
              no_other_feeder = true 
              break
            end

            if Util::alive?(pid)
              no_other_feeder = Process::pid == pid ? true : false
                #no_other_feeder = false 
              #else
                #no_other_feeder = false 
              #end
              break
            else
              no_other_feeder = true 
              STDERR.puts "WARNING : process <#{ pid }> died holding lock on <#{ @pidfilepath }>"
              STDERR.puts "WARNING : attempting autorecovery!"
              break if locked
              STDERR.puts "WARNING : your NFS locking setup is FUBAR - iptables or firewall issues!"
              STDERR.puts "WARNING : attempting autorecovery!"
              FileUtils::rm_f @pidfilepath 
              4.times{ sleep rand }
            end

          rescue Exception => e
            STDERR.puts "WARNING : #{ e.message } (#{ e.class })"
          end
        end


        unless(locked and no_other_feeder)
          pid = Integer(IO::read(@pidfilepath)) rescue 'UNKNOWN' 
          if @options['quiet']
            exit EXIT_FAILURE
          else
            abort "process <#{ pid }> is already feeding from this queue"
          end
        else
          @pidfile.chmod 0600 rescue nil
          @pidfile.rewind
          @pidfile.sync = true
          @pidfile.print Process::pid
          @pidfile.truncate @pidfile.pos
          @pidfile.flush

          at_exit do 
            FileUtils::rm_f @pidfilepath rescue nil
            @pidfile.posixlock File::LOCK_UN rescue nil
            @pidfile.close rescue nil
          end
        end
#--}}}
      end
      def gen_pidfilepath
#--{{{
        # @pidfilepath ||= gen_feeder_name
        @pidfilepath ||= File::join(@dot_rq_dir, 'pid') 
#--}}}
      end
      def gen_feeder_name path = nil
#--{{{
        path ||= (@options['name'] || @qpath)
        path = Util::realpath(path).gsub(%r|/|o, '_')
        #File::join(Util::realpath('~'), ".#{ path }.feeder")
        basename = ".#{ Util::host }_#{ path }.feeder".gsub(%r/_+/,'_')
        dirname = Util::realpath '~'
        File::join dirname, basename
#--}}}
      end
      def install_signal_handlers
#--{{{
        if @daemon or ENV['RQ_SIGNALS']
          $rq_signaled = false
          $rq_sighup = false
          $rq_sigterm = false
          $rq_sigint = false
          trap('SIGHUP') do
            $rq_signaled = $rq_sighup = 'SIGHUP' 
            if nothing_running?
              warn{ "signal <SIGHUP>" }
              handle_signal
            else
              warn{ "finishing running jobs before handling signal" }
            end
          end
          trap('SIGTERM') do
            $rq_signaled = $rq_sigterm = 'SIGTERM' 
            if nothing_running?
              warn{ "signal <SIGTERM>" }
              handle_signal
            else
              warn{ "finishing running jobs before handling signal" }
            end
          end
          trap('SIGINT') do
            $rq_signaled = $rq_sigint = 'SIGINT' 
            if nothing_running?
              warn{ "signal <SIGINT>" }
              handle_signal
            else
              warn{ "finishing running jobs before handling signal" }
            end
          end
          @jrd.install_signal_handlers
        else
          %w(SIGHUP SIGTERM SIGINT).each do |sig|
            trap(sig) do
              warn{ "signal <#{ sig }>" }
              warn{ "not cleaning up - only daemon mode cleans up!" }
              exit
            end
          end
        end
#--}}}
      end
      def install_redirects
#--{{{
        if @daemon
          open('/dev/null','r+') do |f|
            STDIN.reopen f 
            STDOUT.reopen f 
            STDERR.reopen f 
          end
        end
#--}}}
      end
      def fill_morgue
#--{{{
        debug{ "filling morgue..." }
        transaction do
          deadjobs = @q.getdeadjobs @started
          deadjobs.each do |job|
            @q.jobisdead job
            unless job['restartable']
              info{ "burried job <#{ job['jid'] }>" }
            else
              warn{ "dead job <#{ job['jid'] }> will be restarted" }
            end
          end
        end
        debug{ "filled morgue" }
#--}}}
      end
      def reap_zombie_ios 
#--{{{
        debug{ "reaping zombie ios" }
        begin
          transaction do
            stdin, stdout, stderr, data = @q.stdin, @q.stdout, @q.stderr, @q.data
            jids = @q.execute("select jid from jobs").map{|tuple| Integer tuple.first}
            jids = jids.inject({}){|h,jid| h.update jid => true}
            %w[ stdin stdout stderr data ].each do |d|
              Dir::glob(File::join(@q.send(d), "*")).each do |iof|
                begin
                  jid = Integer iof[%r/\d+\s*$/]
                  unless jids[jid]
                    debug{ "removing zombie io <#{ iof }>" }
                    FileUtils::rm_rf iof
                  end
                rescue
                  next
                end
              end
            end
          end
        rescue Exception => e # because this is a non-essential function
          warn{ e }
        end
        debug{ "reaped" }
#--}}}
      end
      def handle_signal
#--{{{
        if $rq_sigterm or $rq_sigint
          reap_jobs(reap_only = true) until nothing_running? 
          info{ "** STOPPING **" }
          @jrd.shutdown rescue nil
          @pidfile.posixlock File::LOCK_UN
          exit EXIT_SUCCESS
        end

        if $rq_sighup
          reap_jobs(reap_only = true) until nothing_running? 
          info{ "** RESTARTING **" }
          info{ "** ARGV <#{ @cmd }> **" }
          begin
            @jrd.shutdown rescue nil
            Util::uncache __FILE__ 
            @pidfile.posixlock File::LOCK_UN
            Util::exec @cmd
          rescue Exception => e
            fatal{"** FAILED TO RESTART! **"}
            fatal{ e }
            exit EXIT_FAILURE
          end
        end
#--}}}
      end
      def throttle rate = @min_sleep 
#--{{{
        if Numeric === rate and rate > 0 
          if defined? @last_throttle_time and @last_throttle_time
            elapsed = Time.now - @last_throttle_time
            timeout = rate - elapsed
            if timeout > 0
              timeout = timeout + rand(rate * 0.10)
              debug{ "throttle rate of <#{ rate }> exceeded - sleeping <#{ timeout }>" }
              sleep timeout
            end
          end
          @last_throttle_time = Time.now 
        end
        yield
#--}}}
      end
      def start_jobs
#--{{{
        debug{ "starting jobs..." }
        n_started = 0 
        transaction do
          until busy?
            break unless((job = @q.getjob))
            start_job job
            n_started += 1 
          end
        end
        debug{ "<#{ n_started }> jobs started" }
        n_started
#--}}}
      end
      def start_job job
#--{{{
        jid, command = job['jid'], job['command']

      #
      # we setup state slightly prematurely so jobrunner will have it availible
      #
        job['state'] = 'running'
        job['started'] = Util::timestamp Time::now
        job['runner'] = Util::hostname 

        job['stdout'] = @q.stdout4 jid
        job['stderr'] = @q.stderr4 jid

        jr = @jrd.runner job
        cid = jr.cid
    
        if jr and cid
          jr.run
          job['pid'] = cid
          @children[cid] = job
          @q.jobisrunning job
          info{ "started - jid <#{ job['jid'] }> pid <#{ job['pid'] }> command <#{ job['command'] }>" }
        else
          error{ "not started - jid <#{ job['jid'] }> command <#{ job['command'] }>" }
        end
    
        cid
#--}}}
      end
      def nothing_running?
#--{{{
        @children.size == 0
#--}}}
      end
      def reap_jobs reap_only = false, blocking = true
#--{{{
        debug{ "reaping jobs..." }
        reaped = []

        cid = status = nil
    
        if blocking
          if busy? or reap_only
            cid, status = @jrd.waitpid2 -1, Process::WUNTRACED 
          else
            loop do
              debug{ "not busy - busywait loop" }
              cid, status = @jrd.waitpid2 -1, Process::WNOHANG | Process::WUNTRACED 
              break if cid
              start_jobs unless $rq_signaled
              break if busy?
              cid, status = @jrd.waitpid2 -1, Process::WNOHANG | Process::WUNTRACED 
              break if cid
              sleep 4.2 
            end
            cid, status = @jrd.waitpid2 -1, Process::WUNTRACED unless cid
          end
        else
          cid, status = @jrd.waitpid2 -1, Process::WNOHANG | Process::WUNTRACED 
        end
    
        if cid and status
          job = @children[cid]
          finish_job job, status
    
          transaction do
            loopno = 0
            loop do
              @q.jobisdone job
              @children.delete cid
              reaped << cid
    
              start_jobs unless reap_only or $rq_signaled
    
              if @children.size == 0 or loopno > 42
                sleep 8 if loopno > 42 # wow - we are CRANKING through jobs so BACK OFF!!
                break
              else
                sleep 0.1
                cid, status = @jrd.waitpid2 -1, Process::WNOHANG | Process::WUNTRACED 
                break unless cid and status
                job = @children[cid]
                finish_job job, status
              end
              loopno += 1
            end
          end
        end
        debug{ "<#{ reaped.size }> jobs reaped" }
        reaped
#--}}}
      end
      def finish_job job, status
#--{{{
        job['finished'] = Util::timestamp(Time::now)
        job['elapsed'] = Util::stamptime(job['finished']) - Util::stamptime(job['started'])
        t = status.exitstatus rescue nil 
        job['exit_status'] = t 
        job['state'] = 'finished' 
        if t and t == 0
          info{ "finished - jid <#{ job['jid'] }> pid <#{ job['pid'] }> exit_status <#{ job['exit_status'] }>" }
        else
          warn{ "finished - jid <#{ job['jid'] }> pid <#{ job['pid'] }> exit_status <#{ job['exit_status'] }>" }
        end
#--}}}
      end
      def transaction
#--{{{
        ret = nil
        if @in_transaction
          ret = yield
        else
          begin
            @in_transaction = true
            @q.transaction{ ret = yield }
          ensure
            @in_transaction = false 
          end
        end
        ret
#--}}}
      end
      def busy?
#--{{{
        @children.size >= @max_feed
#--}}}
      end
      def relax
#--{{{
        seconds = rand(@max_sleep - @min_sleep + 1) + @min_sleep
        debug{ "relaxing <#{ seconds }>" }
        sleep seconds
#--}}}
      end
#--}}}
    end # class Feeder
#--}}}
  end # module RQ
$__rq_feeder__ = __FILE__ 
end
