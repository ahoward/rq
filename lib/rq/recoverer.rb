unless defined? $__rq_recoverer__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    class  Recoverer < MainHelper
#--{{{
      def recover
#--{{{
        set_q

        bool = @q.recover! ? true : false
        puts "---"
        puts "recovered : #{ bool }" 

        EXIT_SUCCESS
#--}}}
      end
      alias recover! recover
#--}}}
    end # class Recoverer 
#--}}}
  end # module RQ
$__rq_recoverer__ = __FILE__ 
end
