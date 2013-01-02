#/usr/bin/env python

# TestThreadPool.py
#
# Usage: python -m unittest <module>

"TestThreadPool.py - Unit test for the ThreadPool.py"

import unittest
import thread_pool

from debug_sym import debug

# Enable debugging.
if debug:
	try:
		import pdb
		pdb.set_trace()
	except ImportError, e:
		print "Import unsuccessful."


class TestThreadPool(unittest.TestCase):
	def setUp(self):
		self.pool = thread_pool.ThreadPool()
		self.tasks = []
		assert isinstance(self.pool, thread_pool.ThreadPool)

		tmp_l = range(40)
		for i in tmp_l: # use the default maximum threads.
			if i % 2 == 0:
				tmp = thread_pool.Process(runMethod1, self.pool)
				self.assertTrue(isinstance(tmp, thread_pool.Process))
				self.tasks.append(tmp)
			else:
				tmp = thread_pool.Process(runMethod, self.pool)
				self.assertTrue(isinstance(tmp, thread_pool.Process))
				self.tasks.append(tmp)
			self.log()
		del tmp_l
	
	#@unittest.expectedFailure
	def tearDown(self):
		self.pool.execute_tasks()
		self.log()
		self.assertEqual(self.pool.waitingPSize(), 0, "Waiting pool not empty.")
		self.assertEqual(self.pool.pSize(), 0, "Execution pool not empty.")

	def testAddProcess(self):
		assert len(self.tasks) > 0
		self.pool.addProcesses(self.tasks)
		self.assertEqual(len(self.tasks) - self.pool.waitingPSize(), 
				 self.pool.pSize())

	def log(self):
		print "\n"
		print "%d-%d" % (self.pool.waitingPSize(), self.pool.pSize())
		print "\n"


import time

# The method that each task will run.
def runMethod1():
	time.sleep(2)

def runMethod():
	time.sleep(3)


def suite():
	suite = unittest.TestSuite()
	suite.addTest(TestThreadPool)
	suite.addTest(TestThreadPool)
	return suite

if __name__ == '__main__':
	pdb.runcall(unittest.main)
