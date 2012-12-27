#/usr/bin/env python

# TestThreadPool.py
#
# Usage: python -m unittest <module>

"TestThreadPool.py - Unit test for the ThreadPool.py"

import unittest
import thread_pool


class TestThreadPool(unittest.TestCase):
	def setUp(self):
		self.pool = thread_pool.ThreadPool()
		self.tasks = []
		assert isinstance(self.pool, thread_pool.ThreadPool)

		tmp_l = range(30)
		for i in tmp_l: # use the default maximum threads.
			if i % 2 == 0:
				tmp = thread_pool.Task(runMethod1, self.pool)
				self.assertTrue(isinstance(tmp, thread_pool.Task))
				self.tasks.append(tmp)
			else:
				tmp = thread_pool.Task(runMethod, self.pool)
				self.assertTrue(isinstance(tmp, thread_pool.Task))
				self.tasks.append(tmp)
			self.log()
		del tmp_l
		
	def tearDown(self):
		self.pool.execute_tasks()
		self.log()
		self.assertEqual(self.pool.waitingPSize(), 0, "Waiting pool not empty.")
		self.assertEqual(self.pool.pSize(), 0, "Execution pool not empty.")

	def testAddTasks(self):
		assert len(self.tasks) > 0
		self.pool.addTasks(self.tasks)
		self.assertEqual(len(self.tasks) - self.pool.waitingPSize(), 
				 self.pool.pSize())

	def log(self):
		print "\n"
		print "%d-%d" % (self.pool.waitingPSize(), self.pool.pSize())
		print "\n"


# The method that each task will run.
def runMethod1():
	flag = True
	i = 0
	while flag:
		if i > 1000000:
			flag = not flag
		else:
			i = i + 1

def runMethod():
	flag = True
	i = 0
	while flag:
		if i > 1000000:
			flag = not flag
		else:
			i = i + 3

def suite():
	suite = unittest.TestSuite()
	suite.addTest(TestThreadPool)
	suite.addTest(TestThreadPool)
	return suite

if __name__ == '__main__':
	unittest.main()
