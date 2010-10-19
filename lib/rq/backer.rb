unless defined? $__rq_backer__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # a Backer object makes an (optionally) timestamped hot backup/snapshot of a
    # queue using a timestamp of milli second resolution.
    #
    class  Backer < MainHelper
#--{{{
      def backup 
#--{{{
        set_q
        bak = @argv.shift
        bak ||= "#{ @qpath }.#{ Util::timestamp.gsub(/[:\s\.-]/,'_') }.bak"
        raise "<#{ bak }> exists" if bak and test(?e, bak)
        debug{ "bak <#{ bak }>" }
        @q.lock{ FileUtils::cp_r @qpath, bak }
        info{ "created backup <#{ bak }>" }
#--}}}
      end
#--}}}
    end # class Backer
#--}}}
  end # module RQ
$__rq_backer__ = __FILE__ 
end
