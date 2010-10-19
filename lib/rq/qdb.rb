unless defined? $__rq_qdb__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'arrayfields'

    require LIBDIR + 'util'
    require LIBDIR + 'logging'
    require LIBDIR + 'sleepcycle'
    require LIBDIR + 'refresher'

    #
    # the QDB class is the low level access point to the actual sqlite database.
    # the primary function if performs is to serialize access to the queue db
    # via the locking protocol
    #
    class  QDB
#--{{{
      include Util
      include Logging

      class RollbackTransactionError < StandardError; end
      class AbortedTransactionError < StandardError; end
    
      FIELDS = 
#--{{{
      %w(
        jid priority state 
        submitted started finished elapsed
        submitter runner
        stdin stdout stderr data
        pid exit_status
        tag restartable command
      )
#--}}}
    
      PRAGMAS =
#--{{{
        <<-sql
          PRAGMA default_synchronous = FULL;
        sql
#--}}}
    
      SCHEMA = 
#--{{{
        <<-sql
          create table jobs
          (
            jid integer primary key,
            #{ FIELDS[1..-1].join ",\n          " }
          );
          create table attributes
          (
            key,
            value,
            primary key (key)
          );
        sql
#--}}}
    
      DEFAULT_LOGGER                         = Logger::new(STDERR)
      DEFAULT_SQL_DEBUG                      = false
      DEFAULT_TRANSACTION_RETRIES            = 4 
      DEFAULT_AQUIRE_LOCK_SC                 = SleepCycle::new(2, 16, 2)
      DEFAULT_TRANSACTION_RETRIES_SC         = SleepCycle::new(8, 24, 8)
      DEFAULT_ATTEMPT_LOCKD_RECOVERY         = true
      DEFAULT_LOCKD_RECOVER_WAIT             = 3600  # 1 hr
      DEFAULT_AQUIRE_LOCK_LOCKFILE_STALE_AGE = 21600 # 6 hrs
      DEFAULT_AQUIRE_LOCK_REFRESH_RATE       = 30
    
      class << self
#--{{{
        attr :sql_debug, true
        attr :transaction_retries, true
        attr :aquire_lock_sc, true
        attr :transaction_retries_sc, true
        attr :attempt_lockd_recovery, true
        attr :lockd_recover_wait, true
        attr :aquire_lock_lockfile_stale_age, true
        attr :aquire_lock_refresh_rate, true

        def fields
#--{{{
          FIELDS
#--}}}
        end
        def integrity_check dbpath
#--{{{
          ret = false 
          tuple = nil
          begin
            db = 
              begin
                SQLite::Database::new dbpath, 0
              rescue
                SQLite::Database::new dbpath
              end
            opened = true
            db.use_array = true rescue nil
            tuple = db.execute 'PRAGMA integrity_check;'
            ret = (tuple and tuple.first and (tuple.first["integrity_check"] =~ /^\s*ok\s*$/io))
          ensure
            db.close if opened
            db = nil
          end
          ret
#--}}}
        end
        def t2h tuple
#--{{{
          h = {}
          FIELDS.each_with_index{|f,i| h[f] = tuple[i]}
          h
#--}}}
        end
        def h2t h
#--{{{
          t = tuple
          FIELDS.each{|f| t[f] = h[f]}
          t
#--}}}
        end
        def tuple
#--{{{
          t = Array::new FIELDS.size
          t.fields = FIELDS
          t
#--}}}
        end
        def q tuple
#--{{{
          [ tuple ].flatten.map do |f| 
            if f and not f.to_s.empty?
              "'" << Util.escape(f,"'","'") << "'"
            else
              'NULL'
            end
          end
#--}}}
        end
        def create path, opts = {}
#--{{{
          qdb = new path, opts
          FileUtils::touch qdb.lockfile
          create_schema qdb.schema
          qdb.transaction do 
            qdb.execute PRAGMAS
            qdb.execute SCHEMA
          end
          qdb
#--}}}
        end
        def create_schema path
#--{{{
          tmp = "#{ path }.tmp"
          open(tmp,'w') do |f| 
            f.puts PRAGMAS 
            f.puts SCHEMA
          end
          FileUtils::mv tmp, path
#--}}}
        end
#--}}}
      end
    
      attr :path
      attr :opts
      attr :dirname
      attr :schema
      attr :fields
      attr :mutex
      attr :lockfile
      attr :sql_debug, true
      attr :transaction_retries, true
      attr :aquire_lock_sc, true
      attr :transaction_retries_sc, true
      attr :attempt_lockd_recovery, true
      attr :lockd_recover_wait, true
      attr :aquire_lock_lockfile_stale_age, true
      attr :aquire_lock_refresh_rate, true


      def initialize path, opts = {}
#--{{{
        @path = path
        @opts = opts

        @logger = 
          Util::getopt('logger', @opts) || 
          klass.logger || 
          DEFAULT_LOGGER

        @sql_debug = 
          Util::getopt('sql_debug', @opts) || 
          klass.sql_debug || 
          ENV['RQ_SQL_DEBUG'] || 
          DEFAULT_SQL_DEBUG

        @transaction_retries = 
          Util::getopt('transaction_retries', @opts) || 
          klass.transaction_retries ||
          DEFAULT_TRANSACTION_RETRIES

        @aquire_lock_sc =
          Util::getopt('aquire_lock_sc', @opts) ||
          klass.aquire_lock_sc ||
          DEFAULT_AQUIRE_LOCK_SC

        @transaction_retries_sc = 
          Util::getopt('transaction_retries_sc', @opts) ||
          klass.transaction_retries_sc ||
          DEFAULT_TRANSACTION_RETRIES_SC

        @attempt_lockd_recovery = 
          Util::getopt('attempt_lockd_recovery', @opts) ||
          klass.attempt_lockd_recovery ||
          DEFAULT_ATTEMPT_LOCKD_RECOVERY

        @lockd_recover_wait = 
          Util::getopt('lockd_recover_wait', @opts) ||
          klass.lockd_recover_wait ||
          DEFAULT_LOCKD_RECOVER_WAIT

        @aquire_lock_lockfile_stale_age = 
          Util::getopt('aquire_lock_lockfile_stale_age', @opts) ||
          klass.aquire_lock_lockfile_stale_age ||
          DEFAULT_AQUIRE_LOCK_LOCKFILE_STALE_AGE

        @aquire_lock_refresh_rate = 
          Util::getopt('aquire_lock_refresh_rate', @opts) ||
          klass.aquire_lock_refresh_rate ||
          DEFAULT_AQUIRE_LOCK_REFRESH_RATE


        @schema = "#{ @path }.schema"
        @dirname = File::dirname(path).gsub(%r|/+\s*$|,'')
        @basename = File::basename(path)
        @waiting_w = File::join(@dirname, "#{ Util::hostname }.#{ $$ }.waiting.w") 
        @waiting_r = File::join(@dirname, "#{ Util::hostname }.#{ $$ }.waiting.r") 
        @lock_w = File::join(@dirname, "#{ Util::hostname }.#{ $$ }.lock.w") 
        @lock_r = File::join(@dirname, "#{ Util::hostname }.#{ $$ }.lock.r") 
        @lockfile = File::join(@dirname, 'lock') 
        @lockf = Lockfile::new("#{ @path }.lock") 
        @fields = FIELDS
        @in_transaction = false
        @in_ro_transaction = false
        @db = nil

        @lockd_recover = "#{ @dirname }.lockd_recover"
        @lockd_recover_lockf = Lockfile::new "#{ @lockd_recover }.lock"
        @lockd_recovered = false
#--}}}
      end
      def ro_transaction(opts = {}, &block)
#--{{{
        opts['read_only'] = true
        transaction(opts, &block)
#--}}}
      end
      def transaction opts = {} 
#--{{{
        raise 'nested transaction' if @in_transaction
        ro = Util::getopt 'read_only', opts 
        ret = nil
        begin 
          @in_transaction = true
          lockd_recover_wrap(opts) do
            transaction_wrap(opts) do
              aquire_lock(opts) do
                #sillyclean(opts) do
                  connect do
                    execute 'begin' unless ro
                    ret = yield 
                    execute 'commit' unless ro
                  end
                #end
              end
            end
          end
        ensure
          @in_transaction = false
        end
        ret
#--}}}
      end
if false
      def ro_transaction(opts = {}, &block)
#--{{{
        opts['read_only'] = true
        transaction(opts, &block)
#--}}}
      end
      def transaction opts = {} 
#--{{{
        ro = Util::getopt 'read_only', opts 
        ret = nil
        if @in_transaction
      STDERR.puts 'continuing transaction...'
          ret = yield
        else
          begin 
      STDERR.puts 'starting transaction...'
            @in_transaction = true
            lockd_recover_wrap(opts) do
              transaction_wrap(opts) do
                aquire_lock(opts) do
                  #sillyclean(opts) do
                    connect do
                      execute 'begin' unless ro
                      ret = yield 
                      execute 'commit' unless ro
                    end
                  #end
                end
              end
            end
          ensure
            @in_transaction = false
          end
        end
        ret
#--}}}
      end
end
      def lockd_recover_wrap opts = {}
#--{{{
        ret = nil
        try_again = false
        begin
          begin
            @lockd_recovered = false
            old_mtime = 
              begin
                Util::uncache @lockd_recover rescue nil
                File::stat(@lockd_recover).mtime
              rescue
                Time::now 
              end
            ret = yield
          ensure
            new_mtime =
              begin
                Util::uncache @lockd_recover rescue nil
                File::stat(@lockd_recover).mtime
              rescue
                old_mtime
              end

            if new_mtime and old_mtime and new_mtime > old_mtime and not @lockd_recovered
              try_again = true
            end
          end
        rescue
          if try_again
            warn{ "a remote lockd recovery has invalidated this transaction!" }
            warn{ "retrying..."}
            sleep 120
            retry
          else
            raise
          end
        end
        ret
#--}}}
      end
#
# TODO - perhaps should not retry on SQLException??  yet errors seem to map to
# this exception even when the sql is fine... safest (and most anoying) is to
# simply retry.
#
      def transaction_wrap opts = {} 
#--{{{
        ro = Util::getopt 'read_only', opts
        ret = nil
        if ro
          ret = yield 
        else
          errors = []
          @transaction_retries_sc.reset
          begin
            ret = yield 
          rescue => e
          #rescue SQLite::DatabaseException, SQLite::SQLException, SystemCallError => e
            case e
              when AbortedTransactionError 
                raise
              when RollbackTransactionError 
                raise
              else
                if @transaction_retries == 0
                  raise
                elsif errors.size >= @transaction_retries
                  error{ "MAXIMUM TRANSACTION RETRIES SURPASSED" }
                  raise
                else
                  warn{ e } if(errors.empty? or not Util::erreq(errors[-1], e))
                  errors << e
                  warn{ "retry <#{ errors.size }>..." }
                end
                sleep @transaction_retries_sc.next
                retry
              end
          end
        end
        ret
#--}}}
      end
      def abort_transaction(*a)
#--{{{
        raise AbortedTransactionError, *a
#--}}}
      end
      def rollback_transaction(*a)
#--{{{
        raise RollbackTransactionError, *a
#--}}}
      end
      def sillyclean opts = {} 
#--{{{
        ro = Util::getopt 'read_only', opts
        ret = nil
        if ro
          ret = yield
        else
          glob = File::join @dirname,'.nfs*'
          orgsilly = Dir[glob]
          ret = yield
          newsilly = Dir[glob]
          silly = newsilly - orgsilly 
          silly.each{|path| FileUtils::rm_rf path}
        end
        ret
#--}}}
      end
      def aquire_lock opts = {} 
#--{{{
        ro = Util::getopt 'read_only', opts
        ret = nil

        @aquire_lock_sc.reset
    
        waiting, ltype, lfile =
          if ro
            [@waiting_r, File::LOCK_SH | File::LOCK_NB, @lock_r]
          else
            [@waiting_w, File::LOCK_EX | File::LOCK_NB, @lock_w]
          end
    
        ltype_s = (ltype == File::LOCK_EX ? 'write' : 'read')
        ltype ||= File::LOCK_NB

        aquired = false

        until aquired
          begin
            debug{ "aquiring lock" }
            #@lockf.lock unless ro
      
            open(@lockfile, 'a+') do |lf|

              locked = false
              refresher = nil
              sc = nil

              begin
                FileUtils::touch waiting
                # poll
                42.times do
                  locked = lf.posixlock(ltype | File::LOCK_NB)
                  break if locked
                  sleep rand
                end

                if locked
                  aquired = true
                  refresher = Refresher::new @lockfile, @aquire_lock_refresh_rate
                  debug{ "refresher pid <#{ refresher.pid }> refresh_rate <#{ @aquire_lock_refresh_rate }>" }
                  FileUtils::rm_f waiting rescue nil
                  FileUtils::touch lfile rescue nil
                  debug{ "aquired lock" }
                  ret = yield
                  debug{ "released lock" }
                else
                  aquired = false 
                  stat = File::stat @lockfile
                  mtime = stat.mtime
                  stale = mtime < (Time::now - @aquire_lock_lockfile_stale_age)
                  if stale
                    Util::uncache @lockfile rescue nil
                    stat = File::stat @lockfile
                    mtime = stat.mtime
                    stale = mtime < (Time::now - @aquire_lock_lockfile_stale_age)
                    if stale
                      warn{ "detected stale lockfile of mtime <#{ mtime }>" }
                      lockd_recover if @attempt_lockd_recovery
                    end
                  end
                  sc = @aquire_lock_sc.next
                  debug{ "failed to aquire lock - sleep(#{ sc })" }
                  sleep sc 
                end

              ensure
                if locked
                  unlocked = false
                  begin
                    42.times do
                      unlocked = lf.posixlock(File::LOCK_UN | File::LOCK_NB)
                      break if unlocked
                      sleep rand
                    end
                  ensure
                    lf.posixlock File::LOCK_UN unless unlocked
                  end
                end
                refresher.kill if refresher
                FileUtils::rm_f waiting rescue nil
                FileUtils::rm_f lfile rescue nil 
              end
            end
          ensure
            #@lockf.unlock rescue nil unless read_only
          end
        end
        ret
#--}}}
      end
      def connect
#--{{{
        ret = nil
        opened = nil
        begin
          raise 'db has no schema' unless test ?e, @schema
          debug{"connecting to db <#{ @path }>..."}
          $db = @db = 
            begin
              SQLite::Database::new(@path, 0)
            rescue
              SQLite::Database::new(@path)
            end
          debug{"connected."}
          opened = true
          @db.use_array = true rescue nil
          ret = yield @db
        ensure
          @db.close if opened
          $db = @db = nil
          debug{"disconnected from db <#{ @path }>"}
        end
        ret
#--}}}
      end
      def execute sql, &block
#--{{{
        raise 'not in transaction' unless @in_transaction
        if @sql_debug
          logger << "SQL:\n#{ sql }\n"
        end
        #ret = retry_if_locked{ @db.execute sql, &block }
        ret = @db.execute sql, &block
        if @sql_debug and ret and Array === ret and ret.first
          logger << "RESULT:\n#{ ret.first.inspect }\n...\n"
        end
        ret
#--}}}
      end
#
# TODO - add sleep cycle if this ends up getting used
#
      def retry_if_locked
#--{{{
        ret = nil
        begin
          ret = yield 
        rescue SQLite::BusyException
          warn{ "database locked - waiting(1.0) and retrying" }
          sleep 1.0 
          retry
        end
        ret
#--}}}
      end
      def vacuum
#--{{{
        raise 'nested transaction' if @in_transaction
        begin 
          @in_transaction = true
          connect{ execute 'vacuum' }
        ensure
          @in_transaction = false
        end
        self
#--}}}
      end
      def recover!
#--{{{
        raise 'nested transaction' if @in_transaction
        begin 
          @in_transaction = true
          connect{ execute 'vacuum' }
          require 'timeout'
          Timeout::timeout(60){ system "sqlite #{ @path } .tables >/dev/null 2>&1" }
        ensure
          @in_transaction = false
        end
        integrity_check
#--}}}
      end
      def lockd_recover
#--{{{
        return nil unless @attempt_lockd_recovery
        warn{ "attempting lockd recovery" }
        time = Time::now
        ret = nil

        @lockd_recover_lockf.lock do
          Util::uncache @dirname rescue nil
          Util::uncache @path rescue nil
          Util::uncache @lockfile rescue nil
          Util::uncache @lockd_recover rescue nil
          mtime = File::stat(@lockd_recover).mtime rescue time

          if mtime > time 
            warn{ "skipping lockd recovery (another node has already recovered)" }
            ret = true
          else
            moved = false
            begin
              FileUtils::touch @lockd_recover 
              @lockd_recovered = false 

              begin
                report = <<-msg
                  hostname : #{ Util::hostname }
                  pid      : #{ Process.pid }
                  time     : #{ Time::now }
                  q        : 
                    path : #{ @dirname }
                    stat : #{ File::stat(@dirname).inspect }
                  db       : 
                    path : #{ @path }
                    stat : #{ File::stat(@path).inspect }
                  lockfile : 
                    path : #{ @lockfile }
                    stat : #{ File::stat(@lockfile).inspect }
                msg
                info{ "LOCKD RECOVERY REPORT" }
                logger << report
                cmd = "mail -s LOCKD_RECOVERY ara.t.howard@noaa.gov <<eof\n#{ report }\neof"
                Util::system cmd
              rescue
                nil
              end

              warn{ "sleeping #{ @lockd_recover_wait }s before continuing..." }
              sleep @lockd_recover_wait 

              tmp = "#{ @dirname }.tmp"
              FileUtils::rm_rf tmp
              FileUtils::mv @dirname, tmp
              moved = true

              rfiles = [@path, @lockfile].map{|f| File::join(tmp,File::basename(f))}
              rfiles.each do |f|
                ftmp = "#{ f }.tmp"
                FileUtils::rm_rf ftmp
                FileUtils::cp f, ftmp 
                FileUtils::rm f 
                FileUtils::mv ftmp, f 
              end

              dbtmp = File::join(tmp,File::basename(@path))

              if integrity_check(dbtmp)
                FileUtils::mv tmp, @dirname
                FileUtils::cp @lockd_recover_lockf.path, @lockd_recover 
                @lockd_recovered = true 
                Util::uncache @dirname rescue nil
                Util::uncache @path rescue nil
                Util::uncache @lockfile rescue nil
                Util::uncache @lockd_recover rescue nil
                warn{ "lockd recovery complete" }
              else
                FileUtils::mv tmp, @dirname
                @lockd_recovered = false 
                error{ "lockd recovery failed" }
              end

              ret = @lockd_recovered 
            ensure
              if moved and not @lockd_recovered and tmp and test(?d, tmp)
                FileUtils::mv tmp, @dirname
              end
            end
          end
        end
        ret
#--}}}
      end
      def integrity_check path = @path
#--{{{
        debug{ "running integrity_check on <#{ path }>" }
        klass.integrity_check(path)  
#--}}}
      end
      def lock opts = {} 
#--{{{
        ret = nil
        lockd_recover_wrap do
          aquire_lock(opts) do
            ret = yield 
          end
        end
        ret
#--}}}
      end
      alias write_lock lock
      alias wlock write_lock
      def read_lock(opts = {}, &block)
#--{{{
        opts['read_only'] = true
        lock opts, &block
#--}}}
      end
      alias rlock read_lock
#--}}}
    end # class QDB
#--}}}
  end # module RQ
$__rq_qdb__ = __FILE__ 
end
