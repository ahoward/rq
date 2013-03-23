unless defined? $__rq_jobrunner__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'drb/drb'
    require 'yaml'

    require LIBDIR + 'util'

    #
    # the JobRunner class is responsible for pre-forking a process/shell in
    # which to run a job.  this class is utilized by the JobRunnerDaemon so
    # processes can be forked via a drb proxy to avoid actual forking during an
    # sqlite transaction - which has undefined behaviour
    #
    class  JobRunner
#--{{{
      $VERBOSE = nil
      include DRbUndumped
      attr :q
      attr :job
      attr :jid
      attr :cid
      attr :shell
      attr :command
      attr :stdin
      attr :stdout
      attr :stderr
      attr :data
      alias pid cid
      def initialize q, job
#--{{{
        @q = q
        @job = job
        @jid = job['jid']
        @command = job['command']
        @shell = job['shell'] || 'bash'
        @sh_like = File::basename(@shell) == 'bash' || File::basename(@shell) == 'sh' 
        @r,@w = IO::pipe

        @env = {}
        @env["PATH"] = [@q.bin, ENV["PATH"]].join(":")
        @job.fields.each do |field|
          key = "RQ_#{ field }".upcase.gsub(%r/\s+/,'_')
          val = @job[field]
          val = File.expand_path(File.join(@q.path,val)) if %w( stdin stdout stderr data).include?(field.to_s)
          @env[key] = "#{ val }"
        end
        @env['RQ'] = File.expand_path @q.path
        @env['RQ_JOB'] = @job.to_hash.to_yaml 

        @stdin = @job['stdin']
        @stdout = @job['stdout']
        @stderr = @job['stderr']
        @data = @job['data']

        @stdin &&= File::join(@q.path, @stdin) # assume path relative to queue 
        @stdout &&= File::join(@q.path, @stdout) # assume path relative to queue 
        @stderr &&= File::join(@q.path, @stderr) # assume path relative to queue
        @data &&= File::join(@q.path, @data) # assume path relative to queue 

        [@stdin, @stdout, @stderr].each do |path|
          FileUtils::mkdir_p(FileUtils::dirname(path)) rescue nil
          FileUtils::touch(path) unless File.exist?(path)
        end

        @cid = 
          Util::fork do
            @env.each{|k,v| ENV[k] = v}
            ENV['RQ_PID'] = "#{ $$ }"
            @w.close
            STDIN.reopen @r
            argv =
              if @sh_like 
                [ [@shell, "__rq_job__#{ @jid }__#{ File::basename(@shell) }__"], '--login' ]
              else
                [ [@shell, "__rq_job__#{ @jid }__#{ File::basename(@shell) }__"], '-l' ]
              end
            exec *argv
          end
        @r.close
#--}}}
      end
      def run
#--{{{
        command = @command.gsub %r/#.*/o, '' # kill comments
        path = @q.bin

        command =
          if @sh_like 
            sin = "0<#{ @stdin }" if @stdin
            sout = "1>#{ @stdout }" if @stdout
            serr = "2>#{ @stderr }" if @stderr
            "( PATH=#{ path }:$PATH #{ command } ;) #{ sin } #{ sout } #{ serr }"
          else
            sin = "<#{ @stdin }" if @stdin
            sout = ">#{ @stdout }" if @stdout
            serr = ">&#{ @stderr }" if @stderr
            "( ( #{ command } ;) #{ sin } #{ sout } ) #{ serr }"
          end

        @w.puts command
        @w.close
#--}}}
      end
#--}}}
    end # class JobRunner
#--}}}
  end # module RQ
$__rq_jobrunner__ = __FILE__ 
end
