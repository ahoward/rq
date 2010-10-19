unless defined? $__rq_resource__
  $__rq_resource__ = __FILE__ 

  module RQ 
#--{{{
    RQ::LIBDIR =
      File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
        defined? RQ::LIBDIR

    RQ::INCDIR =
      File::dirname(RQ::LIBDIR) + File::SEPARATOR unless
        defined? RQ::INCDIR

    require INCDIR + 'rq'

    class Resource
#--{{{
#--}}}
    end # class Resource
#--}}}
  end # module RQ
end
