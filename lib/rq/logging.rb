unless defined? $__rq_logging__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require "logger"

    require LIBDIR + 'util'
    #
    # module which adds logging methods to all RQ classes
    #
    module Logging
#--{{{
    #
    # a module that adds an accessor to Logging objects in ored to fix a bug where
    # not all logging devices are put into sync mode, resulting in improper log
    # rolling.  this is a hack.
    #
      module LoggerExt
#--{{{
        attr :logdev
#--}}}
      end # module LoggerExt
    #
    # implementations of the methods shared by both classes and objects of classes
    # which include Logging
    #
      module LogMethods
#--{{{
        def logger
#--{{{
          if defined?(@logger) and @logger
            @logger
          else
            if Class === self
              @logger = self.default_logger
            else
              @logger = self::class::logger
            end
            raise "@logger is undefined!" unless defined?(@logger) and @logger
            @logger
          end
#--}}}
        end
        def logger= log
#--{{{
          @logger = log
          @logger.extend LoggerExt
          @logger.logdev.dev.sync = true
          @logger
#--}}}
        end
        def debug(*args, &block); logger.debug(*args, &block); end
        def info(*args, &block);  logger.info(*args, &block) ; end
        def warn(*args, &block);  logger.warn(*args, &block) ; end
        def error(*args, &block); logger.error(*args, &block); end
        def fatal(*args, &block); logger.fatal(*args, &block); end
        def logerr e
#--{{{
          if logger.debug?
            error{ Util::errmsg e } 
          else
            error{ Util::emsg e } 
          end
#--}}}
        end
#--}}}
      end # module LogMethods

      module LogClassMethods
#--{{{
        def default_logger
#--{{{
          if defined?(@default_logger) and @default_logger
            @default_logger
          else
            self.default_logger = Logger::new STDERR
            @default_logger = Logger::INFO
            @default_logger.warn{ "<#{ self }> using default logger"}
            @default_logger
          end
#--}}}
        end
        def default_logger= log
#--{{{
          @default_logger = (Logger === log ? log : Logger::new(log))
          @default_logger.extend LoggerExt
          @default_logger.logdev.dev.sync = true
          @default_logger
#--}}}
        end
#--}}}
      end

      EOL    = "\n"
      DIV0   = ("." * 79) << EOL 
      DIV1   = ("-" * 79) << EOL 
      DIV2   = ("=" * 79) << EOL 
      DIV3   = ("#" * 79) << EOL 
      SEC0   = ("." * 16) << EOL 
      SEC1   = ("-" * 16) << EOL 
      SEC2   = ("=" * 16) << EOL 
      SEC3   = ("#" * 16) << EOL 

      class << self
#--{{{
        def append_features c
#--{{{
          ret = super
          c.extend LogMethods
          c.extend LogClassMethods
          ret
#--}}}
        end
#--}}}
      end
      include LogMethods
#--}}}
    end # module Logging
#--}}}
  end # module rq
$__rq_logging__ = __FILE__ 
end
