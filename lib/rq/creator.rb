unless defined? $__rq_creator__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'fileutils'
    require LIBDIR + 'mainhelper'

    #
    # a queue is a directory
    #
    # the Creator class is responsible for initializing the queue directory and
    # all supporting files.  these include:
    # * the sqlite database (binary)
    # * the sqlite database schema description file (text)
    # * the empty sentinel file used for locking (text - empty)
    #
    # it is an error to attempt to initialize a queue which already exists
    #
    class  Creator < MainHelper
#--{{{
      def create 
#--{{{
        raise "q <#{ @qpath }> exists!" if test ?e, @qpath
        @q = JobQueue::create @qpath, 'logger' => @logger

        unless quiet?
          puts '---'
          puts "q: #{ @q.path }"
          puts "db: #{ @q.db.path }"
          puts "schema: #{ @q.db.schema }"
          puts "lock: #{ @q.db.lockfile }"
          puts "bin: #{ @q.bin }"
          puts "stdin: #{ @q.stdin }"
          puts "stdout: #{ @q.stdout }"
          puts "stderr: #{ @q.stderr }"
          puts "data: #{ @q.data }"
        end

        rqmailer = File.join(File.dirname(@program), 'rqmailer')
        if test ?e, rqmailer
          FileUtils.cp rqmailer, @q.bin
        end

        self
#--}}}
      end
#--}}}
    end # class Creator
#--}}}
  end # module RQ
$__rq_creator__ = __FILE__ 
end
