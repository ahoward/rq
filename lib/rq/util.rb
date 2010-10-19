unless defined? $__rq_util__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require 'pathname'
    require 'socket'
    require 'tmpdir'

    #
    # of course - all the rest goes here
    #
    module  Util
#--{{{
      class << self
        def export sym
#--{{{
          sym = "#{ sym }".intern
          module_function sym 
          public sym 
#--}}}
        end
        def append_features c
#--{{{
          super
          c.extend Util 
#--}}}
        end
      end
      def mcp obj
#--{{{
        Marshal.load(Marshal.dump(obj))
#--}}}
      end
      export 'mcp'
      def klass
#--{{{
        self.class
#--}}}
      end
      export 'klass'
      def realpath path
#--{{{
        path = File::expand_path "#{ path }"
        begin
          Pathname::new(path).realpath.to_s
        rescue Errno::ENOENT, Errno::ENOTDIR
          path
        end
#--}}}
      end
      export 'realpath'
      def hashify(*hashes)
#--{{{
        hashes.inject(accum={}){|accum,hash| accum.update hash}
#--}}}
      end
      export 'hashify'
      def getopt opt, hash, default = nil
#--{{{
        key = opt
        return hash[key] if hash.has_key? key

        key = "#{ key }"
        return hash[key] if hash.has_key? key

        key = key.intern 
        return hash[key] if hash.has_key? key

        return default
#--}}}
      end
      export 'getopt'
      def alive? pid
#--{{{
        pid = Integer("#{ pid }")
        begin
          Process.kill 0, pid
          true
        rescue Errno::ESRCH
          false
        end
#--}}}
      end
      export 'alive?'
      def maim(pid, opts = {})
#--{{{
        sigs = getopt('signals', opts) || %w(SIGTERM SIGQUIT SIGKILL) 
        suspend = getopt('suspend', opts) || 4
        pid = Integer("#{ pid }")
        sigs.each do |sig|
          begin
            Process.kill(sig, pid)
          rescue Errno::ESRCH
            return nil
          end
          sleep 0.2
          unless alive?(pid)
            break
          else
            sleep suspend
          end
        end
        not alive?(pid)
#--}}}
      end
      export 'maim'
      def timestamp time = Time.now
#--{{{
        usec = "#{ time.usec }"
        usec << ('0' * (6 - usec.size)) if usec.size < 6 
        time.strftime('%Y-%m-%d %H:%M:%S.') << usec
#--}}}
      end
      export 'timestamp'
      def stamptime string, local = true 
#--{{{
        string = "#{ string }"
        pat = %r/^\s*(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d).(\d\d\d\d\d\d)\s*$/o
        match = pat.match string
        raise ArgumentError, "<#{ string.inspect }>" unless match
        yyyy,mm,dd,h,m,s,u = match.to_a[1..-1].map{|m| m.to_i}
        if local
          Time.local yyyy,mm,dd,h,m,s,u
        else
          Time.gm yyyy,mm,dd,h,m,s,u
        end
#--}}}
      end
      export 'stamptime'
      def escape! s, char, esc
#--{{{
        re = %r/([#{0x5c.chr << esc}]*)#{char}/
        s.gsub!(re) do
          (($1.size % 2 == 0) ? ($1 << esc) : $1) + char 
        end
#--}}}
      end
      export 'escape!'
      def escape s, char, esc
#--{{{
        ss = "#{ s }"
        escape! ss, char, esc
        ss
#--}}}
      end
      export 'escape'
      def fork(*args, &block)
#--{{{
        begin
          verbose = $VERBOSE
          $VERBOSE = nil
          Process::fork(*args, &block)
        ensure
          $VERBOSE = verbose
        end
#--}}}
      end
      export 'fork'
      def exec(*args, &block)
#--{{{
        begin
          verbose = $VERBOSE
          $VERBOSE = nil
          Kernel::exec(*args, &block)
        ensure
          $VERBOSE = verbose
        end
#--}}}
      end
      export 'exec'
      def system(*args, &block)
#--{{{
        begin
          verbose = $VERBOSE
          $VERBOSE = nil
          Kernel::system(*args, &block)
        ensure
          $VERBOSE = verbose
        end
#--}}}
      end
      export 'system'
      def hostname
#--{{{
        @__hostname__ ||= Socket::gethostname
#--}}}
      end
      export 'hostname'
      def host
#--{{{
        @__host__ ||= Socket::gethostname.gsub(%r/\..*$/o,'')
#--}}}
      end
      export 'host'
      def emsg e
#--{{{
        "#{ e.message } - (#{ e.class })"
#--}}}
      end
      export 'emsg'
      def btrace e
#--{{{
        (e.backtrace or []).join("\n")
#--}}}
      end
      export 'btrace'
      def errmsg e
#--{{{
        emsg(e) << "\n" << btrace(e)
#--}}}
      end
      export 'errmsg'
      def erreq a, b
#--{{{
        a.class == b.class and
        a.message == b.message and
        a.backtrace == b.backtrace
#--}}}
      end
      export 'erreq'
      def tmpnam dir = Dir.tmpdir, seed = File::basename($0)
#--{{{
        pid = Process.pid
        path = "%s_%s_%s_%s_%d" % 
          [Util::hostname, seed, pid, Util::timestamp.gsub(/\s+/o,'_'), rand(101010)]
        File::join(dir, path)
#--}}}
      end
      export 'tmpnam'
      def uncache file 
#--{{{
        refresh = nil
        begin
          is_a_file = File === file
          path = (is_a_file ? file.path : file.to_s) 
          stat = (is_a_file ? file.stat : File::stat(file.to_s)) 
          refresh = tmpnam(File::dirname(path))
          File::link path, refresh rescue File::symlink path, refresh
          File::chmod stat.mode, path
          File::utime stat.atime, stat.mtime, path
        ensure 
          begin
            File::unlink refresh if refresh
          rescue Errno::ENOENT
          end
        end
#--}}}
      end
      export 'uncache'
      def columnize buf, width = 80, indent = 0
#--{{{
        column = []
        words = buf.split %r/\s+/o
        row = ' ' * indent
        while((word = words.shift))
          if((row.size + word.size) < (width - 1))
            row << word
          else
            column << row
            row = ' ' * indent
            row << word
          end
          row << ' ' unless row.size == (width - 1)
        end
        column << row unless row.strip.empty?
        column.join "\n"
#--}}}
      end
      export 'columnize'
      def defval var, default = nil
#--{{{
        v = "#{ var }"
        c = "DEFAULT_#{ v }".upcase
        begin
          klass.send(v) || klass.const_get(c)
        rescue NameError
          default
        end
#--}}}
      end
      export 'defval'
      def hms s
#--{{{
        h, s = s.divmod 3600
        m, s = s.divmod 60
        [h.to_i, m.to_i, s]
#--}}}
      end
      export 'hms'
      def which_ruby
#--{{{
        c = ::Config::CONFIG
        realpath( File::join(c['bindir'], c['ruby_install_name']) << c['EXEEXT'] )
#--}}}
      end
      export 'which_ruby'
#--}}}
    end # module Util
#--}}}
  end # module RQ
$__rq_util__ = __FILE__ 
end
