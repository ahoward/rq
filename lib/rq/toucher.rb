unless defined? $__rq_toucher__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'
    require LIBDIR + 'job'

    #
    # nodoc
    #
    class  Toucher < MainHelper
#--{{{
      def touch
#--{{{
        set_q

        @priority = @options['priority']
        debug{ "priority <#{ @priority }>" }

        @tag = @options['tag']
        debug{ "tag <#{ @tag }>" }

        @runner = @options['runner']
        debug{ "runner <#{ @runner }>" }

        @restartable = @options['restartable']
        debug{ "restartable <#{ @restartable }>" }

        @infile = @options['infile'] 
        debug{ "infile <#{ @infile }>" }

        @job_stdin = @options['stdin'] 
        debug{ "job_stdin <#{ @job_stdin }>" }

        @stage = @options['stage']
        debug{ "stage <#{ @stage }>" }

        @data = @options['data'] 
        debug{ "data <#{ @data }>" }

        if job_stdin == '-' and stdin?
          abort "cannot specify both jobs and job input on stdin"
        end

        jobs = [] 

        unless @argv.empty?
          job = Job::new
          job['command'] = @argv.join(' ') 
          job['priority'] = @priority
          job['tag'] = @tag 
          job['runner'] = @runner
          job['restartable'] = @restartable
          job['data'] = @data
          jobs << job
        end

        if @infile
          open(@infile) do |f|
            debug{ "reading jobs from <#{ @infile }>" }
            loadio f, @infile, jobs 
          end
        end

        if jobs.empty? and stdin? 
          debug{ "reading jobs from <stdin>" }
          loadio stdin, 'stdin', jobs 
        end

        abort "no jobs specified!" if jobs.empty?

        init_job_stdin!
        
        state = @stage ? 'holding' : 'pending'

        jobs.each do |job|
          job['state'] = state
          job['priority'] = @priority if @options.has_key?('priority')
          job['tag'] = @tag if @options.has_key?('tag')
          job['runner'] = @runner if @options.has_key?('runner')
          job['restartable'] = @restartable if @options.has_key?('restartable')
          job['stdin'] = @job_stdin if @job_stdin
          job['data'] = @data if @data
        end

      #
      # state + lambdas for submit process...
      #

        list = []

        tmpfile =
          lambda do |basename|
            basename = File.basename basename.to_s
            Tempfile.new "#{ basename }_#{ Process.pid }_#{ rand.to_s }"
          end

        update_job = 
          lambda do |pjob, ujob|
            kvs, jid = {}, pjob['jid']
          # handle stdin
            pstdin, ustdin = pjob['stdin'], ujob['stdin']
            if pstdin || ustdin
              pbuf =
                if pstdin
                  pstdin = @q.standard_in_4 jid
                  IO.read pstdin if test ?e, pstdin
                end
              ubuf =
                if ustdin
                  IO.read ustdin if test ?e, ustdin
                end
#y 'pbuf' => pbuf
#y 'ubuf' => ubuf
              f = ustdin ? open(ustdin,'w') : tmpfile[ustdin]
              begin
                f.write pbuf if pbuf
                f.write ubuf if pbuf
              ensure
                f.close
              end
              kvs['stdin'] = ujob['stdin'] = f.path
#y 'stdin' => ujob['stdin']
            end
          # handle other keys
            allowed = %w( priority runner restartable )
            allowed.each do |key|
              val = ujob[key]
              kvs[key] = val if val
            end
            @q.update(kvs, jid){|updated| list << updated}
          end

        submit_job = 
          lambda do |job|
            @q.submit(job){|submitted| list << submitted}
          end


      #
      # update or submit
      #
        @q.transaction do
          pending = @q.list 'pending'

          pjobs, pcommands = {}, {}

          pending.each do |job|
            jid = job['jid']
            command = job['command'].strip
            tag = job['tag'].to_s.strip
            pjobs[jid] = job
            pcommands[[command, tag]] = jid
          end

          jobs.each do |job|
            jid = job['jid']
            command = job['command'].strip
            tag = job['tag'].to_s.strip
            if((jid = pcommands[[command, tag]]))
              update_job[ pjobs[jid], job ] 
            else
              submit_job[ job ]
            end
          end
        end

        list.each &dumping_yaml_tuples unless @options['quiet']
    
        jobs = nil
        list = nil
        self
#--}}}
      end
#--}}}
    end # class Toucher
#--}}}
  end # module RQ
$__rq_toucher__ = __FILE__ 
end
