unless defined? $__rq_mainhelper__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'tempfile'

    require LIBDIR + 'util'
    require LIBDIR + 'logging'

    #
    # the MainHelper class abstracts some of the common functions the various
    # Main delegates require
    #
    class  MainHelper
#--{{{
      include Util
      include Logging
      attr :main
      attr :argv
      attr :env
      attr :program
      attr :stdin
      attr :job_stdin
      attr :cmd
      attr :options
      attr :qpath
      attr :mode
      attr :quiet
      attr :q
      attr :fields
      attr :dot_rq_dir
      attr :loops

      alias_method 'stdin?', 'stdin'
      alias_method 'job_stdin?', 'job_stdin'
      alias_method 'quiet?', 'quiet'
      def initialize main
#--{{{
        @main = main
        @logger = main.logger
        @argv = main.argv
        @env = main.env
        @program = main.program
        @stdin = main.stdin
        @data = main.data
        @job_stdin = main.job_stdin
        @cmd = main.cmd
        @options = main.options
        @qpath = main.qpath
        @mode = main.mode
        @quiet = main.quiet
        @fields = main.fields
        @dot_rq_dir = main.dot_rq_dir
        @loops = main.loops
        @q = nil 
#--}}}
      end
      def set_q
#--{{{
        raise "q <#{ @qpath }> does not exist" unless test ?d, @qpath
        @q = JobQueue::new @qpath, 'logger' => @logger
        if @options['snapshot']
          ss = "#{ $0 }_#{ Process::pid }_#{ Thread::current.object_id.abs }_#{ rand Time::now.to_i  }".gsub(%r|/|o,'_')
          qtmp = File::join Dir::tmpdir, ss
          @q = @q.snapshot qtmp, @options['retries']
          at_exit{ FileUtils::rm_rf qtmp }
        end
#--}}}
      end
      def loadio io, path, jobs
#--{{{
        while((line = io.gets))
          if line =~ %r/^---\s*$/o
            loaded = YAML::load io 
            raise "no jobs in <#{ path }>" unless 
              Array === loaded and 
              Hash === loaded.first and Hash === loaded.last
            loaded.each{|job| jobs << job}
            loaded = nil
          else
            # line.gsub!(%r/(?:^\s+)|(?:\s+$)|(?:#.*$)/o, '')
            line.strip!
            next if line.empty?
            job = Job::new
            if((m =  %r/^\s*(?:jid\s*=\s*)?(\d+)\s*$/io.match(line)))
              job['jid'] = Integer(m[1])
            else
              job['command'] = line
            end
            jobs << job
          end
        end
#--}}}
      end
      def loadio io, path, jobs
#--{{{
        while((line = io.gets))
          if line =~ %r/^---\s*$/o
            loadyaml io, path, jobs
          else
            # line.gsub!(%r/(?:^\s+)|(?:\s+$)|(?:#.*$)/o, '')
            line.strip!
            next if line.empty?
            job = Job::new
            if((m =  %r/^\s*(?:jid\s*=\s*)?(\d+)\s*$/io.match(line)))
              job['jid'] = Integer(m[1])
            else
              job['command'] = line
            end
            jobs << job
          end
        end
#--}}}
      end
      def loadyaml io, path, jobs
#--{{{
        h = nil
        while((line = io.gets))
          line.strip!
          next if line.empty?
          case line
            when %r/^\s*-\s*$/
              jobs << h if h
              h = {}
            else
              k, v = line.split %r/:/, 2
              k.strip!
              v.strip!
              h[k] = v
          end
        end
        jobs << h if h
#--}}}
      end
      def dumping_yaml_tuples
#--{{{
        fields = nil
        dump = lambda do |tuple|
          puts '---'
          if fields.nil?
            if @fields
              fields = field_match @fields, tuple.fields
            else
              fields = tuple.fields
            end
          end
          dump = lambda do |tuple|
            puts '-'
            fields.each{|f| puts " #{ f }: #{ tuple[ f ] }"}
          end
          dump[tuple]
        end
        lambda{|tuple| dump[tuple]}
#--}}}
      end
      def field_match srclist, dstlist
#--{{{
        fields = dstlist.select do |dst|
          srclist.map do |src|
            re =
              if src =~ %r/^[a-zA-Z0-9_-]+$/
                %r/^#{ src }/i
              else
                %r/#{ src }/i
              end
            src == dst or dst =~ re
          end.any?
        end.uniq
#--}}}
      end
      def init_job_stdin!
#--{{{
        if @job_stdin == '-'
          tmp = Tempfile::new "#{ Process::pid  }_#{ rand 42 }"
          while((buf = STDIN.read(8192))); tmp.write buf; end
          tmp.close
          @job_stdin = tmp.path 
        end
        @job_stdin
#--}}}
      end
#--}}}
    end # class MainHelper
#--}}}
  end # module RQ
$__rq_mainhelper__ = __FILE__ 
end
