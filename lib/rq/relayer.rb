unless defined? $__rq_relayer__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'
    require LIBDIR + 'job'
    require LIBDIR + 'jobrunner'
    require LIBDIR + 'jobrunnerdaemon'
    require LIBDIR + 'jobqueue'

    class  Relayer < MainHelper
#--{{{
      DEFAULT_MIN_SLEEP = 42
      DEFAULT_MAX_SLEEP = 240
      DEFAULT_RELAY     = 16 

      class << self
#--{{{
        attr :min_sleep, true
        attr :max_sleep, true
        attr :relay, true
#--}}}
      end

      def relay 
#--{{{
        daemon do
          gen_pidfile
          @main.init_logging
          @logger = @main.logger

          set_q

        #
        # munge @q/@qpath to set there
        #
          @here = @q
          @qpath = realpath @main.argv.shift
          set_q
          @there = @q
          @q = @here
          @hdb = @here.qdb
          @tdb = @there.qdb

          @pid = Process::pid
          @cmd = @main.cmd 
          @started = Util::timestamp
          @min_sleep = Integer(@options['min_sleep'] || defval('min_sleep'))
          @max_sleep = Integer(@options['max_sleep'] || defval('max_sleep'))
          @relay = Integer(@options['number'] || defval('relay'))
          
          @transactions = {}


          install_signal_handlers

          info{ "** STARTED **" }
          info{ "version <#{ RQ::VERSION }>" }
          info{ "cmd <#{ @cmd }>" }
          info{ "pid <#{ @pid }>" }
          info{ "pidfile <#{ @pidfile.path }>" }
          info{ "here <#{ @here.path }>" }
          info{ "there <#{ @there.path }>" }

          debug{ "mode <#{ @mode }>" }
          debug{ "min_sleep <#{ @min_sleep }>" }
          debug{ "max_sleep <#{ @max_sleep }>" }
          debug{ "relay <#{ @relay }>" }

exit

          loop do
            handle_signal if $rq_signaled
            throttle(@min_sleep) do
              reap_and_sow
              relax
            end
          end
=begin
          loop do
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
=end
        end
#--}}}
      end

      def daemon
#--{{{
        if @options['daemon']
          fork do
            Process::setsid
            fork do
              Dir::chdir(Util.realpath('~'))
              File::umask 0
              open('/dev/null','r+') do |f|
                STDIN.reopen f 
                STDOUT.reopen f 
                STDERR.reopen f 
              end
              @daemon = true
              yield
              exit EXIT_SUCCESS
            end
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
        name ||= gen_relayer_name(@options['name'] || @qpath)
        @pidfile = 
          begin
            open name, File::CREAT | File::EXCL | File::RDWR
          rescue
            open name, File::RDWR
          end
        unless @pidfile and @pidfile.posixlock(File::LOCK_EX | File::LOCK_NB)
          pid = IO::read(name) rescue nil
          pid ||= 'unknown'
          if @options['quiet']
            exit EXIT_FAILURE
          else
            raise "process <#{ pid }> is already relaying from this queue"
          end
        else
          @pidfile.rewind
          @pidfile.sync = true
          @pidfile.print Process::pid
          @pidfile.truncate @pidfile.pos
          at_exit{ FileUtils::rm_f name rescue nil }
        end
#--}}}
      end
      def gen_relayer_name path 
#--{{{
        path = Util::realpath(path).gsub(%r|/|o, '_')
        File::join(Util::realpath('~'), ".#{ path }.relayer")
#--}}}
      end
      def install_signal_handlers
#--{{{
        if @daemon
          $rq_signaled = false
          $rq_sighup = false
          $rq_sigterm = false
          $rq_sigint = false
          trap('SIGHUP') do
            $rq_signaled = $rq_sighup = 'SIGHUP' 
            warn{ "signal <SIGHUP>" }
            warn{ "finishing running jobs before handling signal" }
          end
          trap('SIGTERM') do
            $rq_signaled = $rq_sigterm = 'SIGTERM' 
            warn{ "signal <SIGTERM>" }
            warn{ "finishing running jobs before handling signal" }
          end
          trap('SIGINT') do
            $rq_signaled = $rq_sigint = 'SIGINT' 
            warn{ "signal <SIGINT>" }
            warn{ "finishing running jobs before handling signal" }
          end
          @jrd.install_signal_handlers
        end
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
          @jrd.shutdown rescue nil
          Util::uncache __FILE__ 
          @pidfile.posixlock File::LOCK_UN
          Util::exec @cmd
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
      def transaction
#--{{{
        ret = nil
        if @in_transaction
          ret = yield
        else
          begin
            @in_transaction = true
            @here.transaction do
              @there.transaction do
                ret = yield
              end
            end
          ensure
            @in_transaction = false 
          end
        end
        ret
#--}}}
      end
      def relax
#--{{{
        seconds = rand(@max_sleep - @min_sleep + 1) + @min_sleep
        debug{ "relaxing <#{ seconds }>" }
        sleep seconds
#--}}}
      end

#
# TODO - this will need to map jids here to jids there
#
      def reap_and_sow
#--{{{
        transaction{ reap and sow }
#--}}}
      end
      def reap
#--{{{
        debug{ "reaping finished/dead jobs" }

        sql = <<-sql
          select jid from jobs where or state='running'
        sql
        tuples = hdb.execute sql
        hjids = tuples.map{|t| t['jid']}

        unless jids.empty
          where_clauses = hjids.map{|hjid| "jid=#{ hjid }" }
          where_clause = where_clauses.join ' or '
          sql = <<-sql
            select jid from jobs where state='finished' or state='dead' and (#{ where_clause })
          sql
        end

        debug{ "reaped finished/dead jobs" }
        self
#--}}}
      end
#--}}}
    end # class Relayer 
#--}}}
  end # module RQ
$__rq_relayer__ = __FILE__ 
end
