import os
import sys
base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(base_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

import constants
from dateutil.parser import parse
#from . import redis_helpers

import redis_helpers
import redis


def check_window_exists(redis):
	'''check if the window store is created in the store

	Args:
		redis (class redis.client.Redis)

	Returns:
		bool: False if not exists, True if exists
	'''
	name = constants.WINDOW
	is_created = redis_helpers.__is_nil(redis, name)

	if is_created == 0:
		return False

	else:
		return True


def add_comment_to_window(redis, ts, comment_id):
	'''add comment to WINDOW

	Args:
		redis (class redis.client.Redis)
		ts (str)
		comment_id (str)
	'''

	name = constants.WINDOW

	value = "%s,%s"%(comment_id, ts)

	redis_helpers.__add_to_list(redis, name, value)


def get_comments_from_window(redis):
	'''get all comments from the time window

	Args:
		redis (class redis.client.Redis)

	Returns:
		list of comments in WINDOW
	'''
	name = constants.WINDOW

	items = redis_helpers.__get_all_from_list(redis, name)
	return items


def remove_comment_from_window(redis):
	'''remove comment from WINDOW

	Args:
		redis (class redis.client.Redis)
	'''
	redis_helpers.__remove_from_list(redis)


def remove_comment_from_window_pipeline(pipeline):
	'''remove comment from WINDOW in pipeline manner

	Args:
		pipeline
	'''
	redis_helpers.__remove_from_list_pipeline(pipeline)


def check_window_len(redis):
	'''get the length of WINDOW

	Args:
		redis (class redis.client.Redis)

	Returns:
		int
	'''
	key = constants.WINDOW
	length = redis_helpers.__check_list_len(redis, key)
	return length


# COMMENT
def create_comment_hash(redis, comment_id, user_id, content, timestamp):
	'''create a hash for the comment

	Args:
		redis (class redis.client.Redis)
		comment_id (str)
		user_id (str)
		content (str)
		timestamp (str)
	'''
	key = "Comment:%s"%(comment_id)
	value = dict()
	value["user_id"] = user_id
	value["content"] = content
	value["timestamp"] = timestamp
	redis_helpers.__add_to_hash(redis, key, value)


def remove_comment_hash_pipeline(pipeline, comment_id):
	'''remmove comment hash in pipeline manner

	Args:
		pipeline
		value (str)
	'''

	value = "Comment:%s"%(comment_id)
	redis_helpers.__remove_key_pipeline(pipeline, value)
	pass


# LIKE
def add_like_to_comment(redis, comment_id, user_id):
	'''add like to its liked comment

	Args:
		redis (class redis.client.Redis)
		comment_id (str)
		user_id (str)
	'''
	key = "Likes:%s"%(comment_id)
	redis_helpers.__add_to_set(redis, key, user_id)


if __name__ == '__main__':
	pool_1 = redis.ConnectionPool(host='localhost', port=6399, db=0)
	store = redis.Redis(connection_pool=pool_1)

	add_comment_to_window(store, "2016-04-12", "123")
	add_comment_to_window(store, "2016-04-13", "124")
	add_comment_to_window(store, "2016-04-13", "125")

	pipeline = store.pipeline()

	remove_comment_from_window_pipeline(pipeline)
	remove_comment_from_window_pipeline(pipeline)

	pipeline.execute()

	'''
	create_comment_hash(store, "123", "1234", "hello world", "2016-04-12")
	add_comment_to_window(store, "2016-04-12", "123")
	add_comment_to_window(store, "2016-04-13", "124")
	'''