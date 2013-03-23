unless defined? $__rq_resubmitter__
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
    # either a simple list of commands, one per line, where blank lines and
    # comments (#) are ignored OR it is valid yaml input.  if the Submitter sees
    # the token '---' in the input stream it is assumed the input is yaml.  for
    # an example of valid yaml input examine the output of a Lister using
    #
    #   rq q list
    #
    # the output of other commands, such as that of a Querier may also be used
    # as input to submit
    #
    class  ReSubmitter < MainHelper
#--{{{
      def resubmit
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

        @data = @options['data'] 
        debug{ "data <#{ @data }>" }

        if job_stdin == '-' and stdin?
          abort "cannot specify both jobs and job input on stdin"
        end


        jobs = [] 

        if @infile
          open(@infile) do |f|
            debug{ "reading jobs from <#{ @infile }>" }
            loadio f, @infile, jobs 
          end
        end

        if stdin? 
          debug{ "reading jobs from <stdin>" }
          loadio stdin, 'stdin', jobs 
        end
        jobs.each{|job| @argv << Integer(job['jid'])}

        abort "no jobs specified!" if @argv.empty?

        init_job_stdin!
        
        puts '---'
        @q.transaction do
          jobs = @q.list(*@argv)
          jobs.each do |job|
            job['priority'] = @priority if @options.has_key?('priority')
            job['tag'] = @tag if @options.has_key?('tag')
            job['runner'] = @runner if @options.has_key?('runner')
            job['restartable'] = @restartable if @options.has_key?('restartable')
            job['stdin'] = @job_stdin
            job['data'] = @data
            unless job['state'] =~ %r/running/io
              resubmitted = nil
              @q.resubmit(job){|resubmitted|}
              puts '-'
              resubmitted.fields.each{|f| puts " #{ f }: #{ resubmitted[f] }" }
            end
          end
        end

        jobs = nil
        self
#--}}}
      end
#--}}}
    end # class ReSubmitter
#--}}}
  end # module RQ
$__rq_resubmitter__ = __FILE__ 
end
