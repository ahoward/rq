unless defined? $__rq_configfile__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'yaml'

    #
    # the ConfigFile class is a thin class that munges yaml input and populates
    # itself
    #
    class ConfigFile < ::Hash
#--{{{
      DEFAULT_CONFIG = LIBDIR + 'defaultconfig.txt'

      class << self
        def gen_template(arg = nil)
#--{{{
          @data ||= IO::read(DEFAULT_CONFIG)
          case arg 
            when IO 
              arg.write @data
            when String
              open(arg, 'w'){|f| f.write @data}
            else
              STDOUT.write @data 
          end
          self
#--}}}
        end
        def load_default
#--{{{
          @data ||= IO::read(DEFAULT_CONFIG)
          @default ||= YAML::load(munge(@data)) || {}
#--}}}
        end
        def any(basename, *dirnames)
#--{{{
          config = nil
          dirnames.each do |dirname|
            path = File::join dirname, basename 
            if test ?e, path
              config = self::new(path)
              break
            end
          end
          config || self::new('default') 
#--}}}
        end
        def munge buf
#--{{{
          buf.gsub(%r/\t/o,'  ')
#--}}}
        end
      end
      attr :path
      def initialize path
#--{{{
        @path = nil 
        yaml = nil
        if path.nil? or path and path =~ /^\s*default/io
          yaml = self.class.load_default 
          @path = 'DEFAULT' 
        else path
          yaml = YAML::load(self.class.munge(open(path).read))
          @path = path
        end
        self.update yaml
#--}}}
      end
      def to_hash
#--{{{
        {}.update self
#--}}}
      end
#--}}}
    end # class ConfigFile
#--}}}
  end # module RQ
$__rq_configfile__ = __FILE__ 
end
