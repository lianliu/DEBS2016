import os
import sys
base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(base_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

from . import time_transforms as time
from . import redis_operations_2 as r
import constants
import time as t
import datetime
import math


def check_list(redis, current_ts, d):
	'''check the list of comments if any of them are obsolete

	Args:
		redis (class redis.client.Redis)
		current_ts (str)
		d (int)
	'''
	# compare each from the tail to the current time
	# if larger d, removed
	
	items = r.get_comments_from_window(redis)

	current_time = time.parse_timestamp(current_ts)

	pipeline = redis.pipeline()

	# if size of items is small, we should use while 
	# otherwise, use for iterator 
	for item in reversed(items):
		i = item.decode("utf-8").split(",")

		ts = i[1]
		comment_id = i[0]

		comment_time = time.parse_timestamp(ts)

		diff = time.get_sec_diff(current_time, comment_time)

		print("number of comments before cleanup", r.check_window_len(redis))

		if diff > d:
			# remove from list
			r.remove_comment_from_window_pipeline(pipeline)

			# remove comment hash
			r.remove_comment_hash_pipeline(pipeline, comment_id)

			print("remove %s"%(comment_id))
		
		else:
			break

	pipeline.execute()


# TODO
def output_rank(redis, current_ts):
	''' output the different rank than last one at time current_ts

	Args:
		redis (class redis.client.Redis)
		current_ts (str)

	Returns:
		ranking (tuple)
	'''
	pass


if __name__ == '__main__':
	a = list()
	for i in range(5000):
		a.append(i)

	s = t.time()
	i = len(a) - 1
	count = 0
	while i >= 0:
		if a[i] > 1000:
			i -= 1
			count += 1

		else:
			break

	# 3s		
	print("1", t.time() - s)

	s = t.time()
	for i in reversed(a):
		if i > 1000:
			count += 1

		else:
			break

	# 1.3s		
	print("2", t.time() - s)

	s = t.time()
	for i in a[::-1]:
		if i > 1000:
			count += 1

		else:
			break

	# 2s		
	print("3", t.time() - s)
