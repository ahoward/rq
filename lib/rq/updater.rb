unless defined? $__rq_updater__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # the Updater class reads jids from the command line and then looks for
    # key=value pairs on the command line, stdin, or from infile.  the jids are
    # taken to be jids to update with the key=values pairs scanned
    #
    class  Updater < MainHelper
#--{{{
      def update 
#--{{{
        set_q
        jids = []
        kvs = {} 

        kvs.update "stdin" => job_stdin if job_stdin?

      #
      # scan argv for jids to update 
      #
        list, @argv = @argv.partition{|arg| arg =~ %r/^\s*(?:jid\s*=\s*)?\d+\s*$/}
        list.each{|elem| jids << Integer(elem[%r/\d+/])}
        list, @argv = @argv.partition{|arg| arg =~ %r/^\s*(?:p(?:ending)|h(?:olding))\s*$/}
        list.each{|elem| jids << elem.strip.downcase}
      #
      # scan argv for key=val pairs
      #
        keyeqpat = %r/\s*([^\s=]+)\s*=\s*([^\s]*)\s*$/
        list, @argv = @argv.partition{|arg| arg =~ keyeqpat}
        list.each do |elem|
          m = elem.match(keyeqpat)
          k, v = m[1], m[2]
          k = (k.empty? ? nil : k.strip)
          v = (v.empty? ? nil : v.strip)
          v =
            case v
              when %r/^\s*(?:nil|null?)\s*$/io
                nil
              else
                v
            end
          kvs[k] = v
        end

        unless @argv.empty?
          raise "don't know what to do with crap arguments <#{ @argv.join ' ' }>"
        end

      #
      # scan stdin for jids to update iff in pipeline
      #
        if stdin? 
          #pat = %r/^(?:\s*jid\s*:)?\s*(\d+)\s*$/io
          while((line = stdin.gets))
            case line
              when %r/^(?:\s*jid\s*:)?\s*(\d+)\s*$/io
                jids << Integer($1)
              when %r/^\s*p(?:ending)\s*$/io
                jids << 'pending' 
              when %r/^\s*h(?:olding)\s*$/io
                jids << 'holding' 
              else
                next
            end
          end
        end
        #jids.map!{|jid| jid =~ %r/^\s*\d+\s*$/o ? Integer(jid) : jid}
        #raise "no jids" if jids.empty?
      #
      # if no jids were specified simply update ALL pending and holding jobs
      #
        jids << 'pending' << 'holding' if jids.empty?
      #
      # apply the update
      #
        if @options['quiet'] 
          @q.update(kvs,*jids)
        else
          @q.update(kvs,*jids, &dumping_yaml_tuples)
        end
#--}}}
      end
#--}}}
    end # class Updater
#--}}}
  end # module RQ
$__rq_updater__ = __FILE__ 
end
