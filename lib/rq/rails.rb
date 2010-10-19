if defined?(Rails)
  require 'fileutils'

  module Rails
    class << self
      def qpath *arg
        @qpath = arg.shift unless arg.empty?
        @qpath ||= File.join(RAILS_ROOT, 'q')
        @qpath
      end

      def q
        defined?(@q) ? @q : qinit
      end

      def qinit
        unless test ?e, qpath
          cmd = "rq #{ qpath } create"
          system cmd or abort "cmd <#{ cmd }> failed with <#{ $?.inspect }>" 
          cmd = "cp `which rqmailer` #{ qpath }/bin"
          system cmd or abort "cmd <#{ cmd }> failed with <#{ $?.inspect }>" 
        end
        (( logger = Logger.new STDERR )).level = Logger::FATAL
        @q = RQ::JobQueue.new qpath, 'logger' => logger
        @q.extend qextension 
        @q
      end

      def qextension
        Module.new do
          def rqmailer config, template = nil, submission = {}
            config = YAML.load config if String === config

            ### clean up keys
            config = config.inject({}){|h,kv| k, v = kv; h.update k.to_s => v}
            submission = submission.inject({}){|h,kv| k, v = kv; h.update k.to_s => v}

            command = (config["command"] || config["cmd"]) or raise "no command in <#{ config.inspect }>!"

            mconfig = config["mail"] || config
            attachements = [ mconfig["attach"] ].flatten.compact

            tag = "rqmailer"
            command = "rqmailer ### #{ command }"

            submission["tag"] ||= tag
            submission["command"] = command 

            tmpdir = File.join RAILS_ROOT, 'tmp', Process.pid.to_s, '.rqmailer.d'
            FileUtils.mkdir_p tmpdir
            begin
              open(File.join(tmpdir, 'config'), 'w') do |fd|
                fd.write config.to_yaml
              end
              if template
                open(File.join(tmpdir, 'template'), 'w') do |fd|
                  fd.write template 
                end
              end
              d = File.join(tmpdir, "attachements")
              FileUtils.mkdir_p d 
              attachements.each do |attachment|
                FileUtils.cp attachment, d
              end

              submission["data"] = tmpdir 
              job = nil
              submit(submission){|job|}
              job
            ensure
              FileUtils.rm_rf tmpdir
            end
          end
          alias_method "mailrun", "rqmailer"
        end
      end
    end
  end

end
