unless defined? $__rq_lister__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # the Lister class simply dumps the contents of the queue in valid yaml
    #
    class  Lister < MainHelper
#--{{{
      def list 
#--{{{
        set_q

        @infile = @options['infile'] 
        debug{ "infile <#{ @infile }>" }

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

        @q.qdb.transaction_retries = 1

        @q.list(*@argv, &dumping_yaml_tuples)

        jobs = nil
        self
#--}}}
      end
#--}}}
    end # class Lister
#--}}}
  end # module RQ
$__rq_lister__ = __FILE__ 
end
