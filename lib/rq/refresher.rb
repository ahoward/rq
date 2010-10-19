unless defined? $__rq_refresher__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    #
    # the job of the Refresher is to maintain a _lease_ on a file that has been
    # locked.  the method is simply to touch the file at a certain interval
    # thereby keeping it 'fresh'.  a separate process, vs. a thread, is used for
    # this task to eliminate any chance that the ruby interpreter might put all
    # threads to sleep for some blocking tasks, like fcntl based locks which are
    # used heavily in RQ, resulting in a a prematurely stale lockfile 
    #
    class Refresher
#--{{{
      SIGNALS = %w(SIGTERM SIGINT SIGKILL)
      attr :path
      attr :pid
      attr :refresh_rate
      def initialize path, refresh_rate = 8
#--{{{
        @path = path
        File::stat path
        @refresh_rate = Float refresh_rate
        @pipe = IO::pipe
        if((@pid = Util::fork))
          @pipe.last.close
          @pipe = @pipe.first
          @thread = Thread::new{loop{@pipe.gets}}
          Process::detach @pid
        else
          begin
            pid = Process::pid
            ppid = Process::ppid
            $0 = "#{ path }.refresher.#{ pid }"
            SIGNALS.each{|sig| trap(sig){ raise }}
            @pipe.first.close
            @pipe = @pipe.last
            loop do
              FileUtils::touch @path
              sleep @refresh_rate
              Process::kill 0, ppid
              @pipe.puts pid
            end
          rescue Exception => e
            exit!
          end
        end
#--}}}
      end
      def kill
#--{{{
        begin
          @thread.kill rescue nil
          @pipe.close rescue nil
          SIGNALS.each{|sig| Process::kill sig, @pid rescue nil}
        ensure
=begin
          n = 42
          dead = false
          begin
            n.times do |i|
              Process::kill 0, @pid
              sleep 1
            end
          rescue Errno::ESRCH
            dead = true
          end
          raise "runaway refresher <#{ @pid }> must be killed!" unless dead
=end
        end
#--}}}
      end
#--}}}
    end # class Refresher
#--}}}
  end # module RQ
$__rq_refresher__ = __FILE__ 
end
