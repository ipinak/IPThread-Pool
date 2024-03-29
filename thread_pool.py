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

from debug_sym import debug, enable_debugger

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
# TODO - use the args in the task, so you can pass something upon execution.
#

class ProcessDelegate(object):
	"""
	A task delegate for notifying when a task has completed.
	"""
	def __call__(self):
		pass
	
	def notify(self):
		pass
	
	def __str__(self):
		return "<%s: %s>" % (self.__class__.__name__, hex(id(self)))


# Create two locks for each queue.
pLock = threading.Lock() # pool lock.
consumer = threading.Condition()

class ThreadPool(ProcessDelegate):
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

	def addProcess(self, task):
		"""
		Add the task in the pool to run it, or put it in the
		waiting pool for later execution. The queue is limited 
		from your or by default is 8 tasks. The waiting pool is
		unlimited, since it's implemented using a list.
		"""
		if debug: print "%s.addProcess()" % (self.__class__.__name__)
		
		# Don't need it right now.
		assert isinstance(task, Process)

		# If the queue has more tasks running, than the
		# user specified, then put it in the waiting queue.
		pLock.acquire()
		if self.pool.qsize() < self.processes:
			self.pool.put(task)
			pLock.release()
		else:
			self.waitingPool.put(task)
			pLock.release()

	def addProcesses(self, processes):
		"""
		Add tasks in the pool to run it, or put it in the
		waiting pool for later execution. The queue is limited 
		from your or by default is 8 tasks. The waiting pool is
		unlimited, since it's implemented using a list.
		"""
		if debug: 
			print "%s.addProcesses()" % (self.__class__.__name__)
		map(self.addProcess, processes)

	def execute_tasks(self):
		"""
		Run every task on a different thread.
		"""
		if debug: 
			print "%s.execute_tasks()" % (self.__class__.__name__)
		
		# One condition object here to execute new tasks.
		# Execute in FIFO order.
		consumer.acquire()
		# Get the new item to consume.
		while not self.pool.empty():
			r_task = self.pool.get()
			assert isinstance(r_task, Process), "Not a task"
			r_task.start()
			while self.pool.empty() and not self.waitingPool.empty():
				if debug: print "Waiting..."
				consumer.wait()
		consumer.release()

	def notify(self):
		"""
		Remove the completed task from the list and replace it with a 
		new task. The new task is assigned to the previously running 
		thread.
		"""
		if debug: print "%s.notify()" % (self.__class__.__name__)
		
		# Remove a Process from the waiting pool and put it in the queue.
		pLock.acquire()
		if not self.waitingPool.empty():
			# Get the task that you want to execute.
			newProcess = self.waitingPool.get()
			pLock.release()
			
			consumer.acquire()
			# Make a new item for consumption.
			self.pool.put(newProcess)
			consumer.notifyAll()
			consumer.release()
		else:
			# Keep the thread in the idle pool.
			pLock.release()


class Process(threading.Thread):
	"""
	A simple process to assign your work. It can be used
	individually by invoking start() or but assigning it to a 
	thread pool along with other processes.
	"""
	def __init__(self, function, delegate=None, args=None, callback=None):
		"""
		The constructor of a Process object.
		@param function - the function you want to execute.
		@param delegate - this should be the created pool.
		@param args - the passed arguments for the function.
		@param callback - the callback to execute when the process finishes
		"""
		if debug: print "%s.__init__()" % (self.__class__.__name__)
		if delegate != None:
			assert isinstance(delegate, ProcessDelegate), "You must provide a ProcessDelegate."
		self.function = function
		self.args = args
		self.delegate = delegate
		self.callback = callback
		threading.Thread.__init__(self)

	def __call__(self):
		print "%s.__call__()" % (self.__class__.__name__)
 
	def __str__(self):
		return "<%s: %s>" % (self.__class__.__name__, hex(id(self)))
	
	def __unicode__(self):
		return u"<%s: %s>" % (self.__class__.__name__, hex(id(self)))
		
	def run(self):
		"""
		The subclass of Process should implement this and notify
		the delegate when the execution has finished.
		@warning this should never be called, rather the start()
		"""
		if debug: print "%s.run()" % (self.__class__.__name__)

		# Execute the function with the given arguments.
		res = None
		if self.args != None:
			res = self.function(args)
		else:
			res = self.function()
		
		
		# Get the next process to execute.
		try:
			self.delegate.notify()
		except AttributeError, e:
			pass
		else:
			print "Error - Unknown error occurred"

		try:
			self.callback([res])
		except TypeError, e:
			pass


def runMethod():
	import time
	time.sleep(3)

def callb(*args):
	print "Callbacking..."
	print "%s" % args


#enable_debugger()
if __name__ == "__main__":
	t1 = Process(function=runMethod, callback=callb)
	t1.start()
	
	t2 = Process(function=runMethod)
	t2.start()
