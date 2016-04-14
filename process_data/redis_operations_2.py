# add like to its liked comment
def add_like_to_comment(redis, comment_id, user_id):
	key = "Likes:%s"%(comment_id)
	redis_helpers.__add_to_set(redis, key, user_id)

if __name__ == '__main__':
	pass