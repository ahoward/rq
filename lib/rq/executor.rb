unless defined? $__rq_executor__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # the Executor is for expert use only and executes arbitrary sql on the
    # queue's db.  the reason one should not do this directly with the sqlite
    # command line program is that it will not respect the locking subsystem
    # used in RQ - the Executor method will
    #
    class  Executor < MainHelper
#--{{{
    def execute
#--{{{
      set_q
      sql = @argv.join ' '
      if sql.empty? and stdin? 
        debug{ "reading sql from stdin" }
        while((buf = stdin.gets))
          buf.strip!
          buf.gsub! %r/#.*$/o, ''
          next if buf.empty?
          sql << "#{ buf } "
        end
      end
      abort "no sql to execute" if sql.empty?
      @q.qdb.transaction_retries = 0
      @q.transaction{@q.execute(sql, &dumping_yaml_tuples)}
#--}}}
    end
#--}}}
    end # class Executor
#--}}}
  end # module RQ
$__rq_executor__ = __FILE__ 
end
