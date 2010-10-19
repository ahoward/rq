unless defined? $__rq_orderedautohash__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'orderedhash'

#if false
    class ::OrderedHash
#--{{{
      def initialize(*a, &b)
#--{{{
        super
        @order = []
#--}}}
      end
#--}}}
    end
#end

    class OrderedAutoHash < ::OrderedHash
#--{{{
      def initialize(*args)
#--{{{
        super(*args){|a,k| a[k] = OrderedAutoHash::new(*args)}
#--}}}
      end
      def class
#--{{{
        ::Hash
#--}}}
      end
#--}}}
    end # class OrderedAutoHash
#--}}}
  end # module RQ
$__rq_orderedautohash__ = __FILE__ 
end
