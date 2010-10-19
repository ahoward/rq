unless defined? $__rq_submitter__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'
    require LIBDIR + 'job'

    #
    # the Submitter class is responsible for submitting commands to the queue,
    # the commands it submits are taken from the command line, stdin, or the
    # specified infile.  the format of commands read from stdin or file is
    # either a simple list of commands, one per line, where blank lines are
    # ignored OR it is valid yaml input.  if the Submitter sees the token
    # '---' in the input stream it is assumed the input is yaml.  for an
    # example of valid yaml input examine the output of a Lister using
    #
    #   rq q list
    #
    # the output of other commands, such as that of a Querier may also be used
    # as input to submit
    #
    class  Submitter < MainHelper
#--{{{
      def submit
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

        if @options['quiet'] 
          @q.submit(*jobs)
        else
          @q.submit(*jobs, &dumping_yaml_tuples)
        end
    
        jobs = nil
        self
#--}}}
      end
#--}}}
    end # class Submitter
#--}}}
  end # module RQ
$__rq_submitter__ = __FILE__ 
end
