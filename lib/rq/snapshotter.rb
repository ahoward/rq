unless defined? $__rq_snapshotter__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # a Snapshotter is responsible for safely making a hot snapshot of a queue's
    # db.  it's very useful to make a snapshot if, for instance, you are working
    # out a complex query over several attempts since each attempt will compete
    # with other processes for the queue's lock.  by making a snapshot you will
    # have your own read only copy to perfect your command before applying it to
    # the actual queue.  the feature can also be used to make a hot backup of a
    # queue - tough the Backer has some features that make this more convenient
    #
    class  Snapshotter < MainHelper
#--{{{
      def snapshot 
#--{{{
        set_q
        qtmp = @argv.shift
        raise "<#{ qtmp }> exists" if qtmp and test(?e, qtmp)
        qss = @q.snapshot qtmp, @options['retries']
        #info{ "created q snapshot <#{ qtmp }>" }

        puts '---'
        puts "q: #{ qss.path }"
        puts "db: #{ qss.db.path }"
        puts "schema: #{ qss.db.schema }"
        puts "lock: #{ qss.db.lockfile }"
#--}}}
      end
#--}}}
    end # class Snapshotter
#--}}}
  end # module RQ
$__rq_snapshotter__ = __FILE__ 
end
