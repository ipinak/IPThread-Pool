#!/usr/bin/env python

"""thread_pool.py -- Trying to make a thread pool."""

__author__ = "ipinak"
__version__ = "0.1"
__copyright__ = """
Copyright 1992-2012 The FreeBSD Project. All rights reserved.

Redistribution and use in source and binary forms, with or without 
modification, are permitted provided that the following conditions 
are met:

	Redistributions of source code must retain the above copyright notice, 
	this list of conditions and the following disclaimer.
	Redistributions in binary form must reproduce the above copyright 
	notice, this list of conditions and the following disclaimer in the 
	documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE FREEBSD PROJECT ``AS IS'' AND ANY EXPRESS 
OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES 
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN 
NO EVENT SHALL THE FREEBSD PROJECT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (
	INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
	SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
DAMAGE.

The views and conclusions contained in the software and documentation are 
those of the authors and should not be interpreted as representing official 
policies, either expressed or implied, of the FreeBSD Project.
"""


import threading
import Queue

from debug_sym import debug


# This is set due to the fact, that a lot of CPUs support 
# 4-8 hardware threads though it's not always the case since
# this is true for general purpose computers, rather for 
# servers.
DEFAULT_MAX_PROCESSES = 8

def singleton(cls):
	"""
	Decorate a class with @singleton when There Can Be Only One.
	"""
	instance = cls()
	instance.__call__ = lambda: instance
	return instance

#
# TODO - test execution of the tasks and the delegate functionality. 
# (almost done)
#
# TODO - fix the queuing mechanism for threading. You will setup a number of 
# threads, but the number of tasks should be independent of the number of 
# threads.
# So, you will able to add as many tasks as you like, but the pool will only 
# execute tasks that don't exceed the maximum number of threads.
#
# TODO - use the args in the task, so you can pass something upon execution.
#

class TaskDelegate(object):
	"""
	A task delegate for notifying when a task has completed.
	"""
	def __call__(self):
		pass
	
	def notify(self, thread, result):
		pass
	
	def __repr__(self):
		return "<%s: %s>" % (self.__class__.__name__, hex(id(self)))
	
	def __str__(self):
		return "<%s: %s>" % (self.__class__.__name__, hex(id(self)))


# Create two locks for each queue.
pLock = threading.Lock() # pool lock.
eLock = threading.Lock() # execution lock.
cvProducer = threading.Condition()
cvConsumer = threading.Condition()

class ThreadPool(TaskDelegate):
	"""
	Threading mechanism using a simple pool.
	"""
	def __init__(self, processes=DEFAULT_MAX_PROCESSES):
		if debug: print "%s.__init__()" % (self.__class__.__name__)
		
		self.processes = processes
		# The pool always contains tasks and the waitingPool always
		# contains functions, that will be assigned to tasks.
		self.pool = Queue.Queue()
		self.waitingPool = Queue.Queue()
		
		def processes():
			doc = "The maximum number of processes concurrently running."
			
			def fget(self):
				return self._processes
			
			def fset(self, processes):
				self._processes = processes
				
			def fdel(self):
				del self._processes

			return locals()

		processes = property(**processes())
			
	def __repr__(self):
		return "<%s: %s>" % (self.__class__.__name__, hex(id(self)))

	def __str__(self):
		return "<%s: %s - pool(%d) - waiting(%d) - idleThreads(%d) - processes(%d)>" % (self.__class__.__name__, hex(id(self)), self.waitingPSize(), self.pSize(), self.idlePSize(), self.processes)

	def pSize(self):
		"""
		Return the number of items in the pool.
		"""
		return self.pool.qsize()
		
	def waitingPSize(self):
		"""
		Return the number of items in the waiting pool.
		"""
		return self.waitingPool.qsize()

	def addTask(self, task):
		"""
		Add the task in the pool to run it, or put it in the
		waiting pool for later execution. The queue is limited 
		from your or by default is 8 tasks. The waiting pool is
		unlimited, since it's implemented using a list.
		"""
		if debug: print "%s.addTask()" % (self.__class__.__name__)
		
		# Don't need it right now.
		assert isinstance(task, Task)

		# If the queue has more tasks running, than the
		# user specified, then put it in the waiting queue.
		if debug: print "One step before the lock"
		pLock.acquire()
		if debug: print "Passed lock"

		if self.pool.qsize() < self.processes:
			# Create the running thread either from reusing a thread
			# from that was set to idle or by creating a new one.
			
			#tmp = None
			#try:
			#	if not self.idleThreadPool.empty():
			#		tmp = self.idleThreadPool.get()
			#		assert isinstance(tmp, Task)
			#		tmp.func = task # assign the task to execute.
			#else:
			#		tmp = Task(task, self)
			#except Queue.Empty, e:
			#	print "%s - %s" % (e.status, e.message)
			#self.pool.put(tmp)
			self.pool.put(task)
			pLock.release()
		else:
			self.waitingPool.put(task)
			pLock.release()

	def addTasks(self, tasks):
		"""
		Add tasks in the pool to run it, or put it in the
		waiting pool for later execution. The queue is limited 
		from your or by default is 7 tasks. The waiting pool is
		unlimited, since it's implemented using a list.
		"""
		if debug: 
			print "%s.addTasks()" % (self.__class__.__name__)
		map(self.addTask, tasks)

	def execute_tasks(self):
		"""
		Run every task on a different thread.
		"""
		if debug: 
			print "%s.execute_tasks()" % (self.__class__.__name__)
		
		# One condition object here to execute new tasks.
		# Execute in FIFO order.
		cvConsumer.acquire()
		while self.pool.empty():
			print "Waiting..."
			cvConsumer.wait()
		cvConsumer.release()
		while not self.pool.empty():
			#eLock.acquire()
			r_task = self.pool.get()
			print "before"
			#eLock.release()
			assert isinstance(r_task, Task), "not a task"
			r_task.start()
			print "after"

	def notify(self, thread, result):
		"""
		Remove the completed task from the list and replace it with a 
		new task. The new task is assigned to the previously running 
		thread (improves performance, since we don't have any thread
		creation overhead).
		"""
		if debug: 
			print "%s.notify()" % (self.__class__.__name__)
		# Remove a Task from the waiting pool and put it in the queue.
		pLock.acquire()
		if not self.waitingPool.empty():
			# Get the task that you want to execute.
			newTask = self.waitingPool.get()
			pLock.release()

			pLock.acquire()
			# For the finished thread, assign a new task and then
			# put it back in the pool for execution.
			cvProducer.acquire()
			self.pool.put(newTask)
			cvProducer.notifyAll()
			cvProducer.release()
			print "evale %s" % newTask
			pLock.release()
		else:
			# Keep the thread in the idle pool.
			#self.idleThreadPool.put(thread)
			pLock.release()


# This could be used as a decorator.
class Task(threading.Thread):
	"""
	A simple task to execute. It executes, when you either invoke 
	__call__() the class or execute() method.
	"""
	def __init__(self, func, delegate, args=None):
		if debug: print "%s.__init__()" % (self.__class__.__name__)
		assert isinstance(delegate, TaskDelegate), "You must provide a TaskDelegate."
		self.func = func
		self.args = args
		self.delegate = delegate
		threading.Thread.__init__(self)

	def __call__(self):
		print "%s.__call__()" % (self.__class__.__name__)
 
	def __str__(self):
		return "<%s: %s>" % (self.__class__.__name__, hex(id(self)))
		
	def run(self):
		"""
		The subclass of Task should implement this and notify
		the delegate when the execution has finished.
		"""
		if debug: print "%s.run()" % (self.__class__.__name__)

		# Execute the function with the given arguments.
		res = self.func()
		
		# Pass the task itself and the result after the execution.
		self.delegate.notify(self, res)
