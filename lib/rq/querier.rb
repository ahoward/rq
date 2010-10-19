unless defined? $__rq_queryier__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # a Querier simply takes an sql where clause (such as 'jid = 42') from the
    # command line, queries the queue, and dumps a valid yaml representation of
    # the tuples returned.  the output of a Querier can be used as the input to
    # a Deleter or Submitter, etc.
    #
    #
    class Querier < MainHelper
#--{{{
      def query
#--{{{
        set_q

        @q.qdb.transaction_retries = 1

        where_clause = @argv.join ' '

        if where_clause.empty? and stdin? 
          debug{ "reading where_clause from stdin" }
          while((buf = stdin.gets))
            buf.strip!
            buf.gsub! %r/#.*$/o, ''
            next if buf.empty?
            where_clause << "#{ buf } "
          end
        end

        @q.query(where_clause, &dumping_yaml_tuples)
#--}}}
      end
      def query
#--{{{
        set_q

        @q.qdb.transaction_retries = 1

        simple_pat = %r/^\s*([^=\s!~]+)(=~|!~|!=|==|=)([^=\s]+)\s*$/ox
        simple_query = @argv.select{|arg| arg !~ simple_pat }.empty?

        where_clause = 
          if simple_query 
            wc = [] 

            @argv.each do |arg|
              m = simple_pat.match(arg).to_a[1..-1]
              field, op, value = m[0], m[1], m[2..-1].join 
              op =
                case op
                  when '=', '==' 
                    '='
                  when '!='
                    '!='
                  when '=~'
                    'like'
                  when '!~'
                    'not like'
                end

              quoted = (value =~ %r/^\s*'.*'\s*$/o)
              numeric = begin; Float(value); true; rescue; false; end 

              value = "'#{ value }'" unless quoted or numeric

              wc << "(#{ field } #{ op } #{ value })"
            end

            wc.join ' and '
          else
            @argv.join ' '
          end

        if where_clause.strip.empty? and stdin?
          debug{ "reading where_clause from stdin" }
          while((buf = stdin.gets))
            buf.strip!
            buf.gsub! %r/#.*$/o, ''
            next if buf.empty?
            where_clause << "#{ buf } "
          end
        end

        @q.query(where_clause, &dumping_yaml_tuples)
#--}}}
      end
#--}}}
    end # class Queryier
#--}}}
  end # module RQ
$__rq_queryier__ = __FILE__ 
end
