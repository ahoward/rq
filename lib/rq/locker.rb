unless defined? $__rq_locker__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'util'
    require LIBDIR + 'locker'

    #
    # a Locker simply obtains an exclusive lock on a queue's lock and then runs
    # and arbitrary command which is taken from the command line only.  it's use
    # is simply to allow unforseen applications to coordinate access to the
    # queue
    #
    class  Locker < MainHelper
#--{{{
      def lock
#--{{{
        set_q
        ltype = @argv.shift
        debug{ "ltype <#{ ltype }>" }
        read_only =
          case ltype
            when /^\s*r(?:ead)?|^\s*sh(?:ared)?/io
              true
            when /^\s*w(?:rite)?|^\s*ex(?:clusive)?/io
              false
            else
              raise "lock type must be one of (r)ead|(sh)ared|(w)rite|(ex)clusive, not <#{ ltype }>"
          end
        cmd = @argv.join(' ').strip
        raise "no command given for lock type <#{ ltype }>" if cmd.empty?
        debug{ "cmd <#{ cmd }>" }
        @q.lock(:read_only => read_only){ Util::system cmd }
#--}}}
      end
#--}}}
    end # class Locker
#--}}}
  end # module RQ
$__rq_locker__ = __FILE__ 
end
