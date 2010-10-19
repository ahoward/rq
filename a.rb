require 'rq'

q = RQ::JobQueue.new 'q'

q.submit 'echo 42'

y q.list
