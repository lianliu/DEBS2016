import constants


#TODO private methods
# LIST
# TODO add to list
def __add_to_list(redis, key, value):
	pass


# TODO remove from list
def __remove_from_list(redis, key):
	pass


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

# update field to hash
def __update_field_to_hash(redis, name, field, value):
	redis.hset(name, field, value)


# SET
# add to set
def __add_to_set(redis, key, value):
	redis.sadd(key, value)


# get values from set
def __get_value_from_set(redis, key):
	results = redis.smembers(key)
	return results


# remove from set    
def __remove_from_set(redis, key, value):
	# values = redis.smembers(key)
	# return values
	redis.srem(key, value)


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


# delete elements in sorted set
def __remove_element_from_sorted_set(pipeline, element):
	key = constants.RANKING
	pipeline.zrem(key, element)


# check the score of a particular element in sorted set
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


# get the size of sorted set
def __get_size_sorted_set(redis, name):
	size = redis.zcard(name)
	return size

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