unless defined? $__rq_statuslister__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    require LIBDIR + 'mainhelper'

    #
    # the StatusLister class dumps a yaml report on stdout showing how many jobs
    # are in each of the states
    # * pending
    # * holding 
    # * running 
    # * finished 
    # * dead 
    #
    class  StatusLister < MainHelper
#--{{{
      def statuslist 
#--{{{
        set_q
        exit_code_map = parse_exit_code_map @options['exit']
        puts @q.status('exit_code_map' => exit_code_map).to_yaml
#--}}}
      end
      def parse_exit_code_map emap = 'ok=42'
        emap ||= 'ok=42'
        map = {}
        begin
          tokens = emap.strip.gsub(%r/\s+/, ' ').gsub(%r/\s*=\s*/, '=').split
          tokens.each do |token|
            key, *values = token.split %r/[=,]/
            values.map!{|value| Integer value}
            map[key.to_s] = values
          end
        rescue => e
          warn{ e }
          raise "bad map <#{ emap }"
        end
        map
      end
#--}}}
    end # class StatusLister
#--}}}
  end # module RQ
$__rq_statuslister__ = __FILE__ 
end
