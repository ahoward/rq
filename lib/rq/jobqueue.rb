unless defined? $__rq_jobqueue__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'tempfile'

    require LIBDIR + 'util'
    require LIBDIR + 'logging'
    require LIBDIR + 'qdb'
    require LIBDIR + 'orderedhash'
    require LIBDIR + 'orderedautohash'

    #
    # the JobQueue class is responsible for high level access to the job queue
    #
    class  JobQueue
#--{{{
      include Logging
      include Util
      class Error < StandardError; end
    
      MAX_JID = 2 ** 20 
    
      class << self
#--{{{
        def create path, opts = {}
#--{{{
          FileUtils::rm_rf path
          FileUtils::mkdir_p path
          db = File::join path, 'db'
          qdb = QDB.create db, opts
          opts['qdb'] = qdb
          q = new path, opts
          FileUtils::mkdir_p q.bin
          FileUtils::mkdir_p q.stdin 
          FileUtils::mkdir_p q.stdout
          FileUtils::mkdir_p q.stderr
          FileUtils::mkdir_p q.data
          q
#--}}}
        end
#--}}}
      end

      attr :path
      attr :bin
      attr :stdin
      attr :stdout
      attr :stderr
      attr :data
      attr :opts
      attr :qdb
      alias :db :qdb

      def initialize path, opts = {}
#--{{{
        @path = path # do NOT expand this or it'll be fubar from misc nfs mounts!!
        @bin = File::join @path, 'bin' 
        @stdin = File::join @path, 'stdin' 
        @stdout = File::join @path, 'stdout' 
        @stderr = File::join @path, 'stderr' 
        @data = File::join @path, 'data' 
        @opts = opts
        raise "q <#{ @path }> does not exist" unless test ?e, @path
        raise "q <#{ @path }> is not a directory" unless test ?d, @path
        @basename = File::basename(@path)
        @dirname = File::dirname(@path)
        @logger = getopt('logger', opts) || Logger::new(STDERR)
        @qdb = getopt('qdb', opts) || QDB::new(File::join(@path, 'db'), 'logger' => @logger)
        @in_transaction = false
        @in_ro_transaction = false
#--}}}
      end
      def stdin4 jid
#--{{{
        "stdin/#{ jid }"
#--}}}
      end
      def standard_in_4 jid
#--{{{
        File::expand_path(File::join(path, stdin4(jid)))
#--}}}
      end
      def stdout4 jid
#--{{{
        "stdout/#{ jid }"
#--}}}
      end
      def standard_out_4 jid
#--{{{
        File::expand_path(File::join(path, stdout4(jid)))
#--}}}
      end
      def stderr4 jid
#--{{{
        "stderr/#{ jid }"
#--}}}
      end
      def standard_err_4 jid
#--{{{
        File::expand_path(File::join(path, stderr4(jid)))
#--}}}
      end
      def data4 jid
#--{{{
        "data/#{ jid }"
#--}}}
      end
      def data_4 jid
#--{{{
        File::expand_path(File::join(path, data4(jid)))
#--}}}
      end
      def submit(*jobs, &block)
#--{{{
        if jobs.size == 1 and jobs.first.is_a?(String)
          jobs = [ { "command" => jobs.to_s } ]
        end

        now = Util::timestamp Time::now
    
        transaction do
          sql = "select max(jid) from jobs"
          tuple = execute(sql).first
          jid = tuple.first || 0
          jid = Integer(jid) + 1

          jobs.each do |job|
            command = job['command']
            stdin = job['stdin']
            data = job['data']

            raise "no command for job <#{ job.inspect }>" unless command 

            tmp_stdin(stdin) do |ts|
              tuple = QDB::tuple

              tuple['command']     = command 
              tuple['priority']    = job['priority'] || 0
              tuple['tag']         = job['tag']
              tuple['runner']      = job['runner']
              tuple['restartable'] = job['restartable']
              tuple['state']       = 'pending'
              tuple['submitted']   = now
              tuple['submitter']   = Util::hostname
              tuple['stdin']       = stdin4 jid
              tuple['stdout']      = nil 
              tuple['stderr']      = nil 
              tuple['data']       = data4 jid

              values = QDB::q tuple

              sql = "insert into jobs values (#{ values.join ',' });\n"
              execute(sql){}

              FileUtils::rm_rf standard_in_4(jid)
              FileUtils::rm_rf standard_out_4(jid)
              FileUtils::rm_rf standard_err_4(jid)
              FileUtils::rm_rf data_4(jid)
              FileUtils::cp ts.path, standard_in_4(jid) if ts
              if data
                FileUtils::cp_r data, data_4(jid)
              else
                FileUtils::mkdir_p data_4(jid)
              end

              if block
                sql = "select * from jobs where jid = '#{ jid }'"
                execute(sql, &block)
              end
            end

            jid += 1
          end
        end
    
        self
#--}}}
      end
      def resubmit(*jobs, &block)
#--{{{
        now = Util::timestamp Time::now

        transaction do
          jobs.each do |job|
            jid = Integer job['jid']
            command = job['command']
            stdin = job['stdin']
            data = job['data']

            raise "no jid for job <#{ job.inspect }>" unless jid 
            raise "no command for job <#{ job.inspect }>" unless command 

            tmp_stdin(stdin) do |ts|
              tuple = QDB::tuple

              tuple['jid']         = jid
              tuple['command']     = command
              tuple['priority']    = job['priority'] || 0
              tuple['tag']         = job['tag']
              tuple['runner']      = job['runner']
              tuple['restartable'] = job['restartable']
              tuple['state']       = 'pending'
              tuple['submitted']   = now
              tuple['submitter']   = Util::hostname
              tuple['stdin']       = stdin4 jid
              tuple['stdout']      = nil
              tuple['stderr']      = nil
              tuple['data']        = data4 jid

              kvs = tuple.fields[1..-1].map{|f| "#{ f }=#{ QDB::q(tuple[ f ]) }"}
              sql = "update jobs set #{ kvs.join ',' } where jid=#{ jid };\n"

              execute(sql){}

              FileUtils::rm_rf standard_in_4(jid)
              FileUtils::rm_rf standard_out_4(jid)
              FileUtils::rm_rf standard_err_4(jid)
              #FileUtils::rm_rf data_4(jid)
              FileUtils::cp ts.path, standard_in_4(jid) if ts
              if data
                FileUtils::mv data, data_4(jid)
              else
                FileUtils::mkdir_p data_4(jid)
              end

              if block
                sql = "select * from jobs where jid = '#{ jid }'"
                execute(sql, &block)
              end
            end # tmp_stdin
          end # jobs.each
        end # transaction
    
        self
#--}}}
      end
      def tmp_stdin stdin = nil
#--{{{
        #stdin = nil if stdin.to_s.empty?
        if stdin.to_s.empty?
          return(block_given? ? yield(nil) : nil) 
        end
        stdin = STDIN if stdin == '-'

        was_opened = false

        begin
          unless stdin.respond_to?('read') or stdin.nil?
            stdin = stdin.to_s
            # relative to queue
            if stdin =~ %r|^@?stdin/\d+$|
              stdin.gsub! %r|^@|, ''
              stdin = File::join(path, stdin)
            end
            stdin = File.expand_path stdin
            stdin = open stdin
            was_opened = true
          end

          tmp = Tempfile::new "#{ Process::pid }_#{ rand }"
          while((buf = stdin.read(8192))); tmp.write buf; end if stdin
          tmp.close

          if block_given?
            begin
              yield tmp
            ensure
              tmp.close!
            end
          else
            return tmp
          end
        ensure
          stdin.close if was_opened rescue nil
        end
#--}}}
      end
      def list(*whats, &block)
#--{{{
        ret = nil

        whats.replace(%w( pending running finished dead )) if 
          whats.empty? or whats.include?('all')
    
        whats.map! do |what|
          case what
            when %r/^\s*p/io
              'pending'
            when %r/^\s*h/io
              'holding'
            when %r/^\s*r/io
              'running'
            when %r/^\s*f/io
              'finished'
            when %r/^\s*d/io
              'dead'
            else
              what
          end
        end

        where_clauses = [] 

        whats.each do |what|
          case what
            when Numeric
              where_clauses << "jid=#{ what }\n"
            else
              what = "#{ what }"
              if what.to_s =~ %r/^\s*\d+\s*$/o
                where_clauses << "jid=#{ QDB::q what }\n"
              else
                where_clauses << "state=#{ QDB::q what }\n"
              end
          end
        end

        where_clause = where_clauses.join(" or \n")

        sql = <<-sql
          select * from jobs
          where #{ where_clause } 
        sql

        if block
          ro_transaction{ execute(sql, &block) }
        else
          ret = ro_transaction{ execute(sql) }
        end

        ret
#--}}}
      end
      def status options = {}
#--{{{
        stats = OrderedAutoHash::new

        now = Time::now

        hms = lambda do |t|
          elapsed =
            begin
              Float t
            rescue
              now - Util::stamptime(t, 'local' => true)
            end
          sh, sm, ss = Util::hms elapsed.to_f
          s = "#{ '%2.2d' % sh }h#{ '%2.2d' % sm }m#{ '%05.2f' % ss }s" 
        end

        exit_code_map = 
          options[:exit_code_map] || options['exit_code_map'] || {}

        ro_transaction do
        #
        # jobs stats
        #
          total = 0
          %w( pending holding running finished dead ).each do |state|
            sql = <<-sql
              select count(*) from jobs 
                where 
                  state='#{ state }'
            sql
            tuples = execute sql 
            tuple = tuples.first
            count = (tuple ? Integer(tuple.first || 0) : 0)
            stats['jobs'][state] = count
            total += count
          end
          stats['jobs']['total'] = total
        #
        # temporal stats 
        #
          metrics = OrderedAutoHash::new
          metrics['pending']  = 'submitted'
          metrics['holding']  = 'submitted'
          metrics['running']  = 'started'
          metrics['finished'] = 'elapsed'
          metrics['dead']     = 'elapsed'

          metrics.each do |state, metric|
            sql = 
              unless metric == 'elapsed'
                <<-sql
                  select min(#{ metric }) as max, max(#{ metric }) as min 
                    from jobs where state='#{ state }'
                sql
              else
                <<-sql
                  select min(#{ metric }) as min, max(#{ metric }) as max 
                    from jobs where state='#{ state }'
                sql
              end
            tuple = execute(sql).first
            next unless tuple

            %w( min max ).each do |time|
              oh = nil
              t = tuple[time]
              if t
                sql = <<-sql
                  select jid from jobs where #{ metric }='#{ t }' and state='#{ state }'
                sql
                which = execute(sql).first
                jid = (which and which['jid']).to_i
                if jid
                  oh = OrderedAutoHash::new
                  oh[jid] = hms[t]
                  oh.yaml_inline = true
                end
                stats['temporal'][state][time] = oh 
              end
            end
            #stats['temporal'][state] ||= nil
          end
          stats['temporal'] ||= nil
        #
        # generate performance stats
        #
          sql = <<-sql
            select avg(elapsed) from jobs 
              where 
                state='finished'
          sql
          tuples = execute sql 
          tuple = tuples.first
          avg = (tuple ? Float(tuple.first || 0) : 0)
          stats['performance']['avg_time_per_job'] = hms[avg] 

          list = []
          0.step(5){|i| list << (2 ** i)}
          list << 24
          list.sort!

          list = 1, 12, 24

          list.each do |n|
            ago = now - (n * 3600)
            ago = Util::timestamp ago
            sql = <<-sql
              select count(*) from jobs 
                where 
                  state = 'finished' and 
                  finished  > '#{ ago }'
            sql
            tuples = execute sql 
            tuple = tuples.first
            count = (tuple ? Integer(tuple.first || 0) : 0)
            #stats['performance']["n_jobs_in_last_#{ n }_hrs"] = count
            stats['performance']["n_jobs_in_last_hrs"][n] = count
          end

        #
        # generate exit_status stats
        #
          #stats['exit_status'] = {}
          sql = <<-sql
            select count(*) from jobs 
              where 
                state='finished' and 
                exit_status=0 
          sql
          tuples = execute sql 
          tuple = tuples.first
          successes = (tuple ? Integer(tuple.first || 0) : 0)
          stats['exit_status']['successes'] = successes

          sql = <<-sql
            select count(*) from jobs 
              where 
                (state='finished' and 
                exit_status!=0) or
                state='dead'
          sql
          tuples = execute sql 
          tuple = tuples.first
          failures = (tuple ? Integer(tuple.first || 0) : 0)
          stats['exit_status']['failures'] = failures

          exit_code_map.each do |which, codes|
            exit_status_clause = codes.map{|code| "exit_status=#{ code }"}.join(' or ')
            sql = <<-sql
              select count(*) from jobs 
                where 
                  (state='finished' and (#{ exit_status_clause }))
            sql
            tuples = execute sql 
            tuple = tuples.first
            n = (tuple ? Integer(tuple.first || 0) : 0)
            stats['exit_status'][which] = n
          end
        end

        stats
#--}}}
      end
      def query(where_clause = nil, &block)
#--{{{
        ret = nil

        sql = 
          if where_clause

          #
          # turn =~ into like clauses 
          #
            #where_clause.gsub!(/(=~\s*([^\s')(=]+))/om){q = $2.gsub(%r/'+|\s+/o,''); "like '%#{ q }%'"}
          #
          # quote everything on the rhs of an '=' sign - helps with shell problems...
          #
            #where_clause.gsub!(/(==?\s*([^\s')(=]+))/om){q = $2.gsub(%r/'+|\s+/o,''); "='#{ q }'"}

            "select * from jobs where #{ where_clause };"
          else
            "select * from jobs;"
          end

        if block
          ro_transaction{ execute(sql, &block) }
        else
          ret = ro_transaction{ execute(sql) }
        end

        ret
#--}}}
      end
      def delete(*args, &block)
#--{{{
        whats, optargs = args.partition{|arg| not Hash === arg}

        opts = {}
        optargs.each{|oa| opts.update oa}

        force = Util::getopt 'force', opts

        delete_sql, select_sql = '', ''

        whats << 'all' if whats.empty?

        whats.each do |what|
          case "#{ what }"
            when %r/^\s*\d+\s*$/io # number
              delete_sql << "delete from jobs where jid=#{ what } and state!='running';\n"
              select_sql << "select * from jobs where jid=#{ what } and state!='running';\n"
            when %r/^\s*p/io # pending
              delete_sql << "delete from jobs where state='pending';\n"
              select_sql << "select * from jobs where state='pending';\n"
            when %r/^\s*h/io # holding
              delete_sql << "delete from jobs where state='holding';\n"
              select_sql << "select * from jobs where state='holding';\n"
            when %r/^\s*r/io # running
              delete_sql << "delete from jobs where state='running';\n" if force
              select_sql << "select * from jobs where state='running';\n" if force
            when %r/^\s*f/io # finished
              delete_sql << "delete from jobs where state='finished';\n"
              select_sql << "select * from jobs where state='finished';\n"
            when %r/^\s*d/io # dead
              delete_sql << "delete from jobs where state='dead';\n"
              select_sql << "select * from jobs where state='dead';\n"
            when %r/^\s*a/io # all
              delete_sql << "delete from jobs where state!='running';\n"
              select_sql << "select * from jobs where state!='running';\n"
            else
              raise ArgumentError, "cannot delete <#{ what.inspect }>"
          end
        end

        scrub = lambda do |jid|
          [standard_in_4(jid), standard_out_4(jid), standard_err_4(jid), data_4(jid)].each do |path| 
            FileUtils::rm_rf path
          end
        end

        tuples = []

        metablock = 
          if block
            lambda do |tuple|
              jid = tuple['jid']
              block[tuple]
              scrub[jid]
            end
          else
            lambda do |tuple|
              jid = tuple['jid']
              scrub[jid]
              tuples << tuple
            end
          end

# TODO - make file deletion transactional too

        transaction do
          execute(select_sql, &metablock)
          execute(delete_sql){}
        end

        delete_sql = nil
        select_sql = nil

        block ? nil : tuples
#--}}}
      end
      def vacuum
#--{{{
        @qdb.vacuum
#--}}}
      end
      def update(kvs, *jids, &block)
#--{{{
        ret = nil
      #
      # yank out stdin - which we allow as a key
      #
        stdin = kvs.delete 'stdin'
        data = kvs.delete 'data'
      #
      # validate/munge state value iff present
      #
        if((state = kvs['state']))
          case state
            when %r/^p/io
              kvs['state'] = 'pending'
            when %r/^h/io
              kvs['state'] = 'holding'
            else
              raise "update of <state> = <#{ state }> not allowed (try pending or holding)"
          end
        end
      #
      # validate kvs pairs
      #
        allowed = %w( priority command tag runner restartable )
        kvs.each do |key, val|
          raise "update of <#{ key }> = <#{ val }> not allowed" unless
            (allowed.include?(key)) or (key == 'state' and %w( pending holding ).include?(val))
        end
      #
      # ensure there are acutally some jobs to update
      #
        raise "no jobs to update" if jids.empty?
      #
      # generates sql to update jids with kvs and sql to show updated tuples
      #
        build_sql = 
          lambda do |kvs, jids|
            if(jids.delete('pending'))
              execute("select jid from jobs where state='pending'") do |tuple| 
                jids << tuple['jid']
              end
            end

            if(jids.delete('holding'))
              execute("select jid from jobs where state='holding'") do |tuple| 
                jids << tuple['jid']
              end
            end

            rollback_transaction "no jobs to update" if jids.empty?

            update_clause = kvs.map{|k,v| v ? "#{ k }='#{ v }'" : "#{ k }=NULL" }.join(",\n")
            where_clause = jids.map{|jid| "jid=#{ jid }"}.join(" or\n")
            update_sql = 
              "update jobs\n" <<
              "set\n#{ update_clause }\n" <<
              "where\n(state='pending' or state='holding') and\n(#{ where_clause })"
            select_sql = "select * from jobs where (state='pending' or state='holding') and\n(#{ where_clause })"

            if kvs.empty?
              [ nil, select_sql ]
            else
              [ update_sql, select_sql ]
            end
          end
        #
        # setup stdin
        #
        tmp_stdin(stdin) do |ts|
          clobber_stdin = lambda do |job|
            FileUtils::cp ts.path, standard_in_4(job['jid']) if ts
            true
          end

          clobber_data = lambda do |job|
            if data
              FileUtils::rm_rf data_4(job['jid'])
              FileUtils::cp_r data, data_4(job['jid'])
            end
            true
          end

          tuples = []

          metablock = 
            if block
              lambda{|job| clobber_stdin[job] and clobber_data[job] and block[job]}
            else
              lambda{|job| clobber_stdin[job] and clobber_data[job] and tuples << job}
            end

          transaction do 
            update_sql, select_sql = build_sql[kvs, jids]
            break unless select_sql
            execute(update_sql){} if update_sql
            execute(select_sql, &metablock)
          end

          block ? nil : tuples
        end
#--}}}
      end

      def getjob
#--{{{
        sql = <<-sql
          select * from jobs 
            where 
              (state='pending' or (state='dead' and (not restartable isnull))) and 
              (runner like '%#{ Util::host }%' or runner isnull)
            order by priority desc, submitted asc, jid asc
            limit 1;
        sql
        tuples = execute sql
        job = tuples.first
        job
#--}}}
      end
      def jobisrunning job 
#--{{{
        sql = <<-sql
          update jobs 
            set
              pid='#{ job['pid'] }',
              state='#{ job['state'] }',
              started='#{ job['started'] }',
              runner='#{ job['runner'] }',
              stdout='#{ job['stdout'] }',
              stderr='#{ job['stderr'] }'
            where jid=#{ job['jid'] };
        sql
        execute sql
#--}}}
      end
      def jobisdone job
#--{{{
        sql = <<-sql
          update jobs 
            set
              state = '#{ job['state'] }',
              exit_status = '#{ job['exit_status'] }',
              finished = '#{ job['finished'] }',
              elapsed = '#{ job['elapsed'] }'
            where jid = #{ job['jid'] };
        sql
        execute sql
#--}}}
      end
      def getdeadjobs(started, &block)
#--{{{
        ret = nil
        sql = <<-sql
          select * from jobs 
            where 
              state = 'running' and 
              runner='#{ Util::hostname }' and 
              started<='#{ started }'
        sql
        if block
          execute(sql, &block)
        else
          ret = execute(sql)
        end
        ret
#--}}}
      end
      def jobisdead job
#--{{{
        jid = job['jid']
        if jid
          sql = "update jobs set state='dead' where jid='#{ jid }'"
          execute(sql){}
        end
        job
#--}}}
      end

      def transaction(*args)
#--{{{
        raise "cannot upgrade ro_transaction" if @in_ro_transaction
        ret = nil
        if @in_transaction
          ret = yield
        else
          begin
            @in_transaction = true
            @qdb.transaction(*args){ ret = yield }
          ensure
            @in_transaction = false 
          end
        end
        ret
#--}}}
      end
      def ro_transaction(*args)
#--{{{
        ret = nil
        if @in_ro_transaction || @in_transaction
          ret = yield
        else
          begin
            @in_ro_transaction = true
            @qdb.ro_transaction(*args){ ret = yield }
          ensure
            @in_ro_transaction = false 
          end
        end
        ret
#--}}}
      end
      def execute(*args, &block)
#--{{{
        @qdb.execute(*args, &block)
#--}}}
      end
      def integrity_check(*args, &block)
#--{{{
        @qdb.integrity_check(*args, &block)
#--}}}
      end
      def recover!(*args, &block)
#--{{{
        @qdb.recover!(*args, &block)
#--}}}
      end
      def lock(*args, &block)
#--{{{
        @qdb.lock(*args, &block)
#--}}}
      end
      def abort_transaction(*a,&b)
#--{{{
        @qdb.abort_transaction(*a,&b)
#--}}}
      end
      def rollback_transaction(*a,&b)
#--{{{
        @qdb.rollback_transaction(*a,&b)
#--}}}
      end

      def snapshot qtmp = "#{ @basename }.snapshot", retries = nil 
#--{{{
        qtmp ||= "#{ @basename }.snapshot"
        debug{ "snapshot <#{ @path }> -> <#{ qtmp }>" }
        retries = Integer(retries || 16)
        debug{ "retries <#{ retries }>" }

        qss = nil
        loopno = 0

        take_snapshot = lambda do
          FileUtils::rm_rf qtmp
          FileUtils::mkdir_p qtmp
          %w(db db.schema lock).each do |base|
            src, dest = File::join(@path, base), File::join(qtmp, base)
            debug{ "cp <#{ src }> -> <#{ dest }>" }
            FileUtils::cp(src, dest)
          end
          ss = klass::new qtmp, @opts
          if ss.integrity_check
            ss
          else
            begin; recover! unless integrity_check; rescue; nil; end
            ss.recover!
          end
        end

        loop do
          break if loopno >= retries
          if((ss = take_snapshot.call))
            debug{ "snapshot <#{ qtmp }> created" }
            qss = ss
            break
          else
            debug{ "failure <#{ loopno + 1}> of <#{ retries }> attempts to create snapshot <#{ qtmp }> - retrying..." }
          end
          loopno += 1
        end

        unless qss
          debug{ "locking <#{ @path }> as last resort" }
          @qdb.write_lock do
            if((ss = take_snapshot.call))
              debug{ "snapshot <#{ qtmp }> created" }
              qss = ss
            else
              raise "failed <#{ loopno }> times to create snapshot <#{ qtmp }>"
            end
          end
        end

        qss
#--}}}
      end

# TODO - use mtime to optimize checks by feeder??
      def mtime
#--{{{
        File::stat(@path).mtime
#--}}}
      end
      def []= key, value
#--{{{
        sql = "select count(*) from attributes where key='#{ key }';"
        tuples = @qdb.execute sql
        tuple = tuples.first
        count = Integer tuple['count(*)']
        case count
          when 0
            sql = "insert into attributes values('#{ key }','#{ value }');"
            @qdb.execute sql
          when 1
            sql = "update attributes set key='#{ key }', value='#{ value }' where key='#{ key }';"
            @qdb.execute sql
          else
            raise "key <#{ key }> has become corrupt!"
        end
#--}}}
      end
      def attributes 
#--{{{
        h = {}
        tuples = @qdb.execute "select * from attributes;"
        tuples.map!{|t| h[t['key']] = t['value']}
        h
#--}}}
      end
#--}}}
    end # class JobQueue
#--}}}
  end # module RQ
$__rq_jobqueue__ = __FILE__ 
end
