unless defined? $__rq_configurator__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    # 
    # a Configurator adds key/value pairs to a queue's configuration.  these
    # key/value pairs are not currently used, but will be in a future release 
    # 
    class  Configurator < MainHelper
#--{{{
#--}}}
      def configure
#--{{{
        set_q
        attributes = {}
        unless @argv.empty?
          kv_pat = %r/^\s*([^\s]+)\s*=+\s*([^\s]+)\s*$/o
          @q.transaction do
            @argv.each do |arg|
              match = kv_pat.match arg
              if match 
                k, v = match[1], match[2] 
                @q[k] = v
              end
            end
            attributes = @q.attributes
          end
        end
        y attributes
#--}}}
      end
    end # class Configurator
#--}}}
  end # module RQ
$__rq_configurator__ = __FILE__ 
end
