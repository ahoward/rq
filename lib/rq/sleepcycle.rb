unless defined? $__rq_sleepcycle__
  module RQ 
#--{{{
    LIBDIR = File::dirname(File::expand_path(__FILE__)) + File::SEPARATOR unless
      defined? LIBDIR

    #
    # the sleepcycle class provides timeouts for better than average polling
    # performance by the locking protocol used by the QDB 
    #
    class SleepCycle < Array
#--{{{
      attr :min
      attr :max
      attr :range
      attr :inc
      def initialize min, max, inc
#--{{{
        @min, @max, @inc = Float(min), Float(max), Float(inc)
        @range = @max - @min
        raise RangeError, "max < min" if @max < @min
        raise RangeError, "inc > range" if @inc > @range
        s = @min
        push(s) and s += @inc while(s <= @max)
        self[-1] = @max if self[-1] < @max
        reset
#--}}}
      end   
      def next
#--{{{
        ret = self[@idx]
        @idx = ((@idx + 1) % self.size)
        ret
#--}}}
      end
      def reset
#--{{{
        @idx = 0
#--}}}
      end
#--}}}
    end # class SleepCycle
#--}}}
  end # module RQ
$__rq_sleepcycle__ = __FILE__ 
end
