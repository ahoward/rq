unless defined? $__rq_resourcemanager__
  $__rq_resourcemanager__ = __FILE__ 

  #require 'yaml'
  #require 'yaml/store'
  #require 'socket'

  module RQ 
#--{{{
    RQ::LIBDIR =
      File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
        defined? RQ::LIBDIR

    RQ::INCDIR =
      File::dirname(RQ::LIBDIR) + File::SEPARATOR unless
        defined? RQ::INCDIR

    #require INCDIR + 'rq'

    class ResourceManager
#--{{{
      attr 'path'
      attr 'ystore'

      def initialize path
#--{{{
        @path = File.expand_path path.to_s
        @ystore = YAML::Store.new @path
#--}}}
      end

      def valid? expr
#--{{{
#--}}}
      end
#--}}}
    end # class ResourceManager
#--}}}
  end # module RQ
end
