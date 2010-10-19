unless defined? $__rq_ioviewer__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # the IOViewer class spawns an external editor command to view the
    # stdin/stdout/stderr of a jid(s) 
    #
    class  IOViewer < MainHelper
#--{{{
      def ioview
#--{{{
        @infile = @options['infile'] 
        debug{ "infile <#{ @infile }>" }

        jobs = [] 
        if @infile
          open(@infile) do |f|
            debug{ "reading jobs from <#{ @infile }>" }
            loadio f, @infile, jobs 
          end
        end
        if stdin? 
          debug{ "reading jobs from <stdin>" }
          loadio stdin, 'stdin', jobs 
        end
        jobs.each{|job| @argv << Integer(job['jid'])}

        editor = @options['editor'] || ENV['RQ_EDITOR'] || ENV['RQ_IOVIEW'] || 'vim -R -o'
        @argv.each do |jid|
          jid = Integer jid
          ios = %w( stdin stdout stderr ).map{|d| File.join @qpath, d, jid.to_s}
          command = "#{ editor } #{ ios.join ' ' }"
          system(command) #or error{ "command <#{ command }> failed with <#{ $?  }>" }
        end
        self
#--}}}
      end
#--}}}
    end # class IOViewer
#--}}}
  end # module RQ
$__rq_ioviewer__ = __FILE__ 
end
