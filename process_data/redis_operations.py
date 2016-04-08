import os
import sys
base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(base_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

import redis
import constants


# initialize the store for ranking
# set
def init_ranking_store(redis):
	initial_ranking = ("-", "-", "-")
	ranking_store = constants.RANKING_STORE
	__add_to_set(redis, ranking_store, initial_ranking)


def renew_ranking_store(redis, ranking):
	ranking_store = constants.RANKING_STORE
	redis.delete(ranking_store)
	__add_to_set(redis, ranking_store, ranking)


# TODO need refactoring
# set for ranking
# return 0 if new_ranking is not unique
def add_to_ranking_store(redis, new_ranking):
	ranking_store = constants.RANKING_STORE

	is_success = redis.sadd(ranking_store, new_ranking)
	return is_success


# sorted set for storing scores of posts
def add_to_ranking(redis, post_id):
	score = 10
	key = constants.RANKING
	__add_to_sorted_set(redis, key, post_id, score)


def get_posts_from_ranking(redis):
	key = constants.RANKING
	posts = __get_value_from_sorted_set(redis, key)
	return posts

# remove value from sorted set
# if it expires, when score is equal to or less than 0
def remove_from_ranking(pipeline, element):
	__remove_element_from_sorted_set(pipeline, element)


# increase score of a value in RANKING
def increase_score(redis, value):
	key = constants.RANKING
	amount = 10
	__modify_score(redis, key, value, amount)


# decrease score of a value in RANKING
def decrease_score(redis, value, amount):
	key = constants.RANKING
	amount = -amount
	__modify_score(redis, key, value, amount)


# get score of a value in RANKING
def get_score(redis, value):
	key = constants.RANKING
	score = __check_score(redis, key, value)
	return score

# get top 3 scores from RANKING
def get_top_3_scores(redis):
	results = __get_top_scores(redis, 0, 2)
	return results


# get ts from hash
def find_ts(redis, element_id, flag):
	key = ""

	if flag == "comment":
		key = "Comment:%s"%(element_id)

	elif flag == "post":
		key = "Post:%s"%(element_id) 

	field = "timestamp"
	ts = __find_field(redis, key, field).decode('utf-8')
	return ts


# get last_modified from hash
def find_last_modified(redis, element_id, flag):
	key = ""

	if flag == "comment":
		key = "Comment:%s"%(str(element_id))

	elif flag == "post":
		key = "Post:%s"%(str(element_id))

	field = "last_modified"
	last_modified = __find_field(redis, key, field).decode("utf-8") 
	return last_modified


# update ts from comment hash
def update_ts(redis, element_id, ts, flag):
	key = ""
	value = dict()

	if flag == "comment":
		key = str("Comment:%s"%(element_id))

	elif flag == "post":
		key = str("Post:%s"%(element_id))

	value["timestamp"] = ts
	__add_to_hash(redis, key, value)


# TODO post related
# hash for post
def create_post_hash(redis, post_id, username, timestamp):
	key = "Post:%s"%(post_id)
	value = dict()
	value["username"] = username
	value["timestamp"] = timestamp
	value["last_modified"] = timestamp
	__add_to_hash(redis, key, value)


# find username of a post
def find_username(redis, post_id):
	key = "Post:%s"%(post_id)
	field = "username"
	username = __find_field(redis, key, field).decode('utf-8')
	return username


# clear expired post
def clear_post(pipeline, post_id):
	# 1. clear Post:post_id, hash
	# 2. clear P:post_id, set
	# 3. clear Commenters:post_id, set
	# 4. clear post_id in RANKING, sorted_set
	 
	key_1 = "Post:%s"%(post_id)
	key_2 = "P:%s"%(post_id)
	key_3 = "Commenters:%s"%(post_id)

	__remove_key_pipeline(pipeline, key_1)
	__remove_key_pipeline(pipeline, key_2)
	__remove_key_pipeline(pipeline, key_3)
	remove_from_ranking(pipeline, post_id)


# check if post active
def is_post_active(redis, post_id):
	key = "Post:%s"%(post_id)
	is_post_active = __is_nil(redis, key)
	return is_post_active


# add commenter to a post set
def add_commenter_to_set(redis, post_id, user_id):
	key = "Commenters:%s"%(post_id)
	__add_to_set(redis, key, user_id)


# get number of commenters of a post
def commenters_number(redis, post_id):
	key = "Commenters:%s"%(post_id)
	size = __check_set_size(redis, key)
	return size


# TODO comment related
# hash for comment
def create_comment_hash(redis, comment_id, post_id, content, timestamp):
	key = "Comment:%s"%(comment_id)
	value = dict()
	value["post_id"] = post_id
	value["content"] = content
	value["timestamp"] = timestamp
	value["last_modified"] = timestamp
	__add_to_hash(redis, key, value)


# get post_id from comment hash
def find_post(redis, comment_id):
	key = "Comment:%s"%(comment_id)
	field = "post_id"
	post_id = __find_field(redis, key, field).decode("utf-8")
	return post_id


# set for posts
def add_comment_to_post(redis, post_id, comment_id):
	key = "P:%s"%(post_id)
	__add_to_set(redis, key, comment_id)


# get the comment IDs from post set
def get_comment_from_post_set(redis, post_id):
	key = "P:%s"%(post_id)
	comments = __get_value_from_set(redis, key)
	return comments


# check the size of set in comments of post
def check_comments_number(redis, post_id):
	key = "P:%s"%(post_id)
	size = __check_set_size(redis, key)
	return size


# add like to its liked comment
def add_like_to_comment(redis, comment_id, user_id):
	key = "Likes:%s"%(comment_id)
	__add_to_set(redis, key, user_id)


#TODO private methods
#
# HASH
# add to hash
# value should be a dict
def __add_to_hash(redis, key, value):
	redis.hmset(key, value)


# find value of a field from hash
# return the value of that field
def __find_field(redis, key, field):
	value = redis.hget(key, field)
	return value


# SET
# add to set
def __add_to_set(redis, key, value):
	redis.sadd(key, value)


# get values from set
def __get_value_from_set(redis, key):
	pass


# TODO remove from set    
def __remove_from_set(redis, key):
	values = redis.smembers(key)
	return value


# check the size of set
def __check_set_size(redis, key):
	size = redis.scard(key)
	return size


# SORTED_SET
# add to sorted set
def __add_to_sorted_set(redis, key, element, weight):
	redis.zadd(key, element, weight)


# get value of a sorted set
def __get_value_from_sorted_set(redis, key):
	value = redis.zrange(key, 0, -1)
	return value


# TODO delete elements in sorted set
def __remove_element_from_sorted_set(pipeline, element):
	key = constants.RANKING
	pipeline.zrem(key, element)


# TODO check the score of a particular element in sorted set
def __check_score(redis, key, element):
	score = redis.zscore(key, element)
	return score


# change the score of element in sorted set
def __modify_score(redis, key, value, amount):
	redis.zincrby(key, value, amount)


# get the top scores from sorted set
# return (value, score) pairs in array
def __get_top_scores(redis, start, end):
	key = constants.RANKING
	scores = redis.zrange(key, start, end, desc=True, withscores=True)
	return scores


# REDIS_KEY
# remove key from store by name
def __remove_key(redis, key):
	redis.delete(key)


# remove key from store by name in pipeline
def __remove_key_pipeline(pipeline, key):
	pipeline.delete(key)


# get the key from pattern
# return the value of that key
def __get_key_by_pattern(redis, pattern):
	value = redis.keys(pattern=pattern)
	return value


# check if a key exists
# return 0 if not exist
def  __is_nil(redis, key):
	is_nil = redis.exists(key)
	return is_nil


if __name__ == '__main__':
	pool_1 = redis.ConnectionPool(host='localhost', port=6399, db=0)

	test = redis.Redis(connection_pool=pool_1)

	init_ranking_store(test)

	ranking = ("-", "-", "-" , "-", "-", "-", "-", "-", "-", "-", "-", "-")

	add_to_ranking(test, "123")

	score = get_score(test, "123")

	print(int(score))

	increase_score(test, "123")

	score = get_score(test, "123")

	print(int(score))

	decrease_score(test, "123", 2)

	score = get_score(test, "123")

	print(int(score))
	'''
	num = add_to_ranking_store(test, ranking)
	print(num)

	add_comment_to_post(test, 1, 2)

	remove_key(test, "P:1")

	add_to_hash(test, "123", {"a":123})

	create_post_hash(test, "1234", "lian", "1991-06-22")
	create_comment_hash(test, "1234", "hi", "2016-04-06")


	create_post_hash(test, "123", "abc", "2016-04-07")

	value = find_last_modified(test, "123", "post")
	print(type(value))

	update_ts(test, "123", "2026-04-07", "post")

	'''