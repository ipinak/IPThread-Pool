
import thread_pool


def tt():
   flag = True
   i = 0
   while flag:
      if i == 100000:
         flag = not flag
      else:
         i = i+1

def log():
   print "\n"
   print "%d-%d" % (t.waitingPSize(), t.pSize())
   print "\n"

t = thread_pool.ThreadPool()
for i in range(50):
   t.addTask(thread_pool.Task(tt, t))


log()
t.execute_tasks()
log()
