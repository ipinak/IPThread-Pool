#!/usr/bin/env python

"""thread_pool.py -- Trying to make a thread pool."""

__author__ = "ipinak"
__version__ = "0.1b"
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

DEFAULT_MAX_THREADS = 7

#
# TODO - Fix the pool, when the user updates the maximum number of threads
# 		it should notify the corresponding module or class about the change.
# TODO - test execution of the tasks and the delegate functionality.
#

class TaskDelegate(object):
	"""
	A task delegate for notifying when a task has completed.
	"""
	def __call__(self):
		pass

	def notify(self):
		pass


class ThreadPool(TaskDelegate):
	"""
	Threading mechanism using a simple pool.
	"""
	def __init__(self, maxThreads=DEFAULT_MAX_THREADS):
		if debug: print "%s.__init__()" % self.__class__.__name__
		self.maxThreads = maxThreads
		self.pool = Queue.Queue(maxThreads)
		# TODO - check out if it's better to use a limit for the waiting pool as well.
		self.waitingPool = Queue.Queue()

		def maxThreads():
			doc = "The maximum number of threads property."

			def fget(self):
				return self._maxThreads

			def fset(self, maxThreads):
				self._maxThreads = maxThreads

			def fdel(self):
				del self._maxThreads
			
			return locals()

		maxThreads = property(**maxThreads())

	def pSize():
		"""
		Return the number of items in the pool.
		"""
		return self.pool.qsize()

	def waitingPSize():
		"""
		Return the number of items in the waiting pool.
		"""
		return self.waitingPool.qsize()

	def addTask(self, task):
		"""
		Add the task in the pool to run it, or put it in the
		waiting pool for later execution. The queue is limited 
		from your or by default is 7 tasks. The waiting pool is
		unlimited, since it's implemented using a list.
		"""
		if debug: print "%s.addTask()" % self.__class__.__name__
		if isinstance(task, Task):
			if self.pool.full():
				self.pool.put(task)
			else:
				self.waitingPool.put(task)
		else:
			raise TypeError, "Incorrect type, please provide a Task object."

	def addTasks(self, tasks):
		"""
		Add tasks in the pool to run it, or put it in the
		waiting pool for later execution. The queue is limited 
		from your or by default is 7 tasks. The waiting pool is
		unlimited, since it's implemented using a list.
		"""
		if debug: print "%s.addTasks()" % self.__class__.__name__

		# We want only types of Queue, so if not throw an expection.
		assert isinstance(tasks, Queue.Queue)

		# Apply all the tasks to this function.
		map(self.addTask, tasks)

	def execute_tasks(self):
		"""
		Run every task on a different thread.
		"""
		if debug: print "%s.execute_tasks()" % self.__class__.__name__
		while self.pool.full():
			r_task = self.pool.get()
			# This will invoke __call__() and will run the thread.
			r_task()

	def notify(self, task, res):
		"""
		Remove the completed task from the list and replace it with a new
		task.
		"""
		if debug: print "%s.notify()" % self.__class__.__name__

		# Remove a Task from the waiting pool and put it in the queue.
		if self.waitingPool.qsize() > 0:
			newTask = self.waitingPool.get()
			self.pool.put(newTask)


# This could be used as a decorator.
class Task(threading.Thread):
	"""
	A simple task to execute. The execution is done, when your
	either invoke __call__() the class or execute().
	"""
	def __init__(self, func, delegate, args=None):
		if debug: print self.__class__.__name__
		assert isinstance(delegate, TaskDelegate), "You must provide a TaskDelegate."
		threading.Thread.__init__(self)
		self.func = func
		self.args = args
		self.delegate = delegate

	def __call__(self):
		if debug: print "%s.__call__()" % self.__class__.__name__
		self.run()

	def run(self):
		"""
		The subclass of Task should implement this and notify
		the delegate when the execution has finished.
		"""
		if debug: print "%s.run()" % self.__class__.__name__

		# Execute the function with the given arguments.
		res = apply(self.func, self.args)

		# Pass the task itself and the result after the execution.
		self.delegate.notify(res)


class EmptyException(Exception):
	def __init__(self, name):
		self.name = self.__class__.__name__

	def __str__(self):
		return repr(self.name)


