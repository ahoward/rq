unless defined? $__rq_job__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'arrayfields'
    require 'yaml'

    require LIBDIR + 'util'
    require LIBDIR + 'qdb'

    #
    # Job is a convenience class which stamps out a QDB::tuple and extends it
    # with methods that give accessor methods for each field in the hash 
    #
    class Job < Array
#--{{{
      include ArrayFields
      def initialize kvs = {}
#--{{{
        self.fields = QDB::FIELDS
        (kvs.keys - self.fields).each{|k| self[k] = kvs[k]}
#--}}}
      end
      def method_missing(meth, *args, &block)
#--{{{
        setpat = %r/=$/o
        meth = "#{ meth }"
        setter = meth =~ setpat 
        meth.gsub! setpat, ''
        if fields.include? "#{ meth }"
          if setter
            self.send('[]=', meth, *args, &block)
          else
            self.send('[]', meth, *args, &block)
          end
        else
          super
        end
#--}}}
      end
      def to_yaml
        to_hash.to_yaml
      end
#--}}}
    end # class Job
#--}}}
  end # module RQ
$__rq_job__ = __FILE__ 
end
