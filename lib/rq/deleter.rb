unless defined? $__rq_deleter__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # the Deleter class reads the command line, stdin, or an infile to determine
    # the job ids (jid) of jobs to be deleted from the queue.  jids may be
    # specified on the command line or parsed from stdin or the infile.  any
    # input line matching 'jid : number' or 'number' is taken to be a line
    # indicating a jid to delete.
    #
    class  Deleter < MainHelper
#--{{{
      def delete
#--{{{
        set_q

        whats = @argv

        if whats.empty? and stdin?
          pat = %r/^(?:\s*jid\s*:)?\s*(\d+)\s*$|^\s*(all)\s*$/io
          while((line = stdin.gets))
            match = pat.match line
            next unless match
            whats << (match[1] || match[2])
          end
        end

        #whats.map!{|what| what =~ %r/^\s*\d+\s*$/o ? Integer(what) : what}

        raise "nothing to delete" if whats.empty?

        if @options['quiet'] 
          @q.delete(*whats)
        else
          @q.delete(*whats, &dumping_yaml_tuples)
        end

        @q.vacuum
#--}}}
      end
#--}}}
    end # class Deleter
#--}}}
  end # module RQ
$__rq_deleter__ = __FILE__ 
end
