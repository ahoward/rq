unless defined? $__rq_rotater__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'
    require LIBDIR + 'util'

    class  Rotater < MainHelper
#--{{{
      def rotate
#--{{{
        set_q

        rot = Util::realpath @argv.shift

        rot =
          if rot 
            if File::directory? rot
              t = Time::now.strftime('%Y%m%d%H%M%S')
              File::join(rot, "#{ File.basename @qpath }.#{ t }")
            else
              rot
            end
          else
            t = Time::now.strftime('%Y%m%d%H%M%S')
            "#{ @qpath }.#{ t }"
          end

        #rot ||= "#{ @qpath }.#{ Util::timestamp.gsub(/[:\s\.-]/,'_') }.rot"

        abort "<#{ rot }> exists" if rot and test(?e, rot)
        debug{ "rotation <#{ rot }>" }

        FileUtils::mkdir_p(File::dirname(rot)) rescue nil

        rotq = nil

        @q.transaction do
          begin
            #FileUtils::cp_r @qpath, rot
            self.cp_r @qpath, rot
            rotq = JobQueue::new rot, 'logger' => @logger
            rotq.delete 'pending', 'running', 'holding', 'force' => true
            @q.delete 'dead', 'finished'
          rescue
            FileUtils::rm_rf rot
            raise
          end
        end

        tgz = File::expand_path "#{ rot }.tgz"
        #dirname = File::dirname rot 
        if(system("cd #{ File::dirname rot } && tar cvfz #{ File::basename tgz } #{ File::basename rot }/ >/dev/null 2>&1"))
          FileUtils::rm_rf rot
          rot = tgz
        end

        puts "---"
        puts "rotation : #{ rot }" 

        #puts '---'
        #puts "q: #{ rotq.path }"
        #puts "db: #{ rotq.db.path }"
        #puts "schema: #{ rotq.db.schema }"
        #puts "lock: #{ rotq.db.lockfile }"

        EXIT_SUCCESS
#--}}}
      end
      ##
      # the cp_r should never fail, so we build in another attempt under
      # failing conditions
      def cp_r srcdir, dstdir
#--{{{
      attempts = 0
      loop do
        begin
          break(FileUtils::cp_r(srcdir, dstdir))
        rescue => e
          raise if attempts > 2
          warn{ e }
          Util::uncache srcdir rescue nil
          Util::uncache dstdir rescue nil
          sleep 2
        ensure
          attempts += 1
        end
      end
#--}}}
      end
#--}}}
    end # class Rotater 
#--}}}
  end # module RQ
$__rq_rotater__ = __FILE__ 
end
