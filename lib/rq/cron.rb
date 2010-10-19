unless defined? $__rq_cron__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # a class for managing crontab entries and to start/stop rq
    #
    class Cron < MainHelper
#--{{{
      def initialize *a, &b
#--{{{
        super
        ruby = Util::which_ruby
        this = Util::realpath( File.expand_path( $0 ) )
        q = qpath

        @cmd = "#{ ruby } #{ this } #{ q }"
        @md5 = lambda{|mode| Digest::MD5::hexdigest "#{ @cmd } #{ mode }" }
#--}}}
      end
      def cron 
#--{{{
        which = @argv.shift || 'start'
        which = which.strip.downcase
        #abort "arg not add|start|shutdown|stop" unless %w( start shutdown stop ).include? which
        msg = "cron_#{ which }"
        begin
          send msg
        rescue NoMethodError
          raise ArgumentError, which
        end
        self
#--}}}
      end
      def cron_add
#--{{{
        lines = `crontab -l`.split "\n"

        found = nil

        re = %r/###\s*md5:#{ @md5[:start] }/

        lines.each do |line|
          line.strip!
          next if line[ %r/^\s*#/ ]
          min, hour, dom, mon, dow, entry = line.split %r/\s+/, 6
          next unless entry
          entry.strip!
          entry.gsub! %r/#[^'"]$/, ''
          entry.strip!
          found = re.match entry
          break if found
        end

        unless found
          opts = @options.map{|kv| "'--#{ kv.join('=') }'" }.join(' ')
          entries = [
            "*/15 * * * * #{ @cmd } start #{ opts } ###md5:#{ @md5[:start] }\n",
            "0 0 * * * #{ @cmd } rotate ###md5:#{ @md5[:start] }\n",
          ]
          tmp = Tempfile::new Process::pid.to_s
          lines.each{|line| tmp << "#{ line }\n"}
          entries.each do |entry|
            tmp << entry 
          end
          tmp.close
          system("crontab #{ tmp.path }") or abort("failed to cronify!")
          tmp.close!
          entries.each do |entry|
            puts entry
          end
        end
#--}}}
      end
      def cron_tab
#--{{{
        opts = @options.map{|kv| "'--#{ kv.join('=') }'" }.join(' ')
        entries = [
          "*/15 * * * * #{ @cmd } start #{ opts } ###md5:#{ @md5[:start] }\n",
          "0 0 * * * #{ @cmd } rotate ###md5:#{ @md5[:start] }\n",
        ]
        puts entries
#--}}}
      end
      def cron_start
#--{{{
        cron_add
        #main.start
#--}}}
      end
      def cron_delete
#--{{{
        lines = `crontab -l`.split "\n"

        re = %r/###\s*md5:(#{ @md5[:start] })/
        found = [] 

        lines.each_with_index do |line, idx|
          line.strip!
          next if line[ %r/^\s*#/ ]
          min, hour, dom, mon, dow, entry = line.split %r/\s+/, 6
          next unless entry
          entry.strip!
          entry.gsub! %r/#[^'"]$/, ''
          entry.strip!
          found << idx if(re.match entry)
        end

p found

        unless found.empty?
          deleted = [] 
          found.each{|idx| deleted << lines[idx]; lines.delete_at(idx)} 
          tmp = Tempfile::new Process::pid.to_s
          lines.each{|line| tmp << "#{ line }\n"}
          tmp.close
          system("crontab #{ tmp.path }") or abort("failed to cronify!")
          tmp.close!
          puts deleted
        end
#--}}}
      end
      def cron_shutdown
#--{{{
        cron_delete
        main.shutdown
#--}}}
      end
      def cron_stop
#--{{{
        cron_delete
        main.stop
#--}}}
      end
#--}}}
    end # class Cron 
#--}}}
  end # module RQ
$__rq_cron__ = __FILE__ 
end
