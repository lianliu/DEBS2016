import os
import sys
base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(base_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

import process_data.time_transforms as time
import process_data.redis_operations as r
import constants
import time as t
import datetime
import multiprocessing
import redis as rd


# check comments in a post 
def check_comments_in_post(active_store, post_id, current_ts):
	amount = 0
	comments = r.get_comment_from_post_set(active_store, post_id)
	flag = "comment"

	if comments:
		for comment_id in comments:
			# 1. get ts
			# 2. if current_ts - ts < 10 days
			# 3. 	get last_modified
			# 4. 	diff = current_ts - last_modified
			# 5. 	if diff is more than one day
			# 6. 		amount += diff.days
			# 7. 		update_time
			# 8. else
			# 9. 	number of days = (ts + 10 days) - last_modified
			#10. 	amount += number of days
			#11. 	remove this comment
			#12. update score in RANKING
				
			comment = comment_id.decode("utf-8")

			last_modified = r.find_last_modified(active_store, comment, flag)
			last_modified_time = time.parse_timestamp(last_modified)
			ts = r.find_ts(active_store, comment, flag)

			current_time = time.parse_timestamp(current_ts)
			comment_time = time.parse_timestamp(ts)

			is_active = time.is_active(current_time, comment_time)
			if is_active:

				diff = time.get_days_difference(current_time, last_modified_time)

				if diff >= 1:
					amount += diff

					new_time = time.update_time(last_modified_time, current_time)
					new_timestamp = time.to_timestamp(new_time)
					r.update_ts(active_store, comment, new_timestamp, flag)

			else:
				last_active_time = time.get_last_active_time(comment_time)
				diff = time.get_days_difference(last_active_time, last_modified_time)

				amount += diff

				# remove the comment
				r.remove_comment_from_post(active_store, post_id, comment)

		r.decrease_score(active_store, post_id, amount)


def check_comments(active_store, current_ts):
	# get all active posts from constants.RANKING_STORE
	# check every comment in this post collection
	
	posts = r.get_posts_from_ranking(active_store)

	for post in posts:
		post_id = post.decode('utf-8')

		check_comments_in_post(active_store, post_id, current_ts)	


def check_post(posts):
	pool_1 = rd.ConnectionPool(host='localhost', port=6379, db=0)

	redis = rd.Redis(connection_pool=pool_1)

	flag = "post"

	pipeline = redis.pipeline()

	current_ts = posts[1]

	ps = posts[0]

	for post_id in ps:
		# post is post_id
		# 1. get last_modified
		# 2. get score 
		# 3. diff = current_ts - last_modified
		# 4. if diff.days is more than one day
		# 5. 	amount += diff.days
		# 6. 	if amount >= score
		# 7. 		remove p in RANKING, P:post_id, Post:post_id, Commenters:post_id @active_store	
		# 8.    else 
		# 9. 		update score
		# 10. 		update_time

		# decode when pull results from redis
		post = post_id.decode("utf-8")

		amount = 0

		last_modified = r.find_last_modified(redis, post, flag)

		if current_ts != last_modified:
			ts = r.find_ts(redis, post, flag)
			created_time = time.parse_timestamp(ts)

			current_time = time.parse_timestamp(current_ts)

			last_modified_time = time.parse_timestamp(last_modified)
			score = r.get_score(redis, post)

			post_time = time.parse_timestamp(last_modified)

			days = time.get_days_difference(current_time, post_time)

			# only decrement 10 points from post's contribution
			# even though the post total score would be far more than 10 points
			is_active = time.is_active(current_time, created_time)
			if is_active:
				if days >= 1:
					amount = days

					if amount >= score:
						r.clear_post(pipeline, post)

					else: 
						r.decrease_score(redis, post, amount)
					
						new_time = time.update_time(last_modified_time, current_time)
						new_timestamp = time.to_timestamp(new_time)
						r.update_ts(redis, post, new_timestamp, flag)

			else:
				if days >= 1:
					amount = days

					if amount >= score:
						r.clear_post(pipeline, post)

					else: 
						new_time = time.update_time(last_modified_time, current_time)
						new_timestamp = time.to_timestamp(new_time)
						r.update_ts(redis, post, new_timestamp, flag)

	pipeline.execute()	


def check_posts(redis, current_ts):
	posts = r.get_posts_from_ranking(redis)

	posts_1 = (posts[0:int(len(posts)/4)], current_ts)
	posts_2 = (posts[int(len(posts)/4):int(len(posts)/2)], current_ts)
	posts_3 = (posts[int(len(posts)/2):int(len(posts)/4 * 3)], current_ts)
	posts_4 = (posts[int(len(posts)/4 * 3):], current_ts)

	p_1 = multiprocessing.Process(target=check_post, args=(posts_1,))
	p_2 = multiprocessing.Process(target=check_post, args=(posts_2,))
	p_3 = multiprocessing.Process(target=check_post, args=(posts_3,))
	p_4 = multiprocessing.Process(target=check_post, args=(posts_4,))

	p_1.start()
	p_2.start()
	p_3.start()
	p_4.start()

	p_1.join()
	p_2.join()
	p_3.join()
	p_4.join()


# get the new ranking 
def get_new_ranking(ranking_with_scores):
	len_of_rank = len(ranking_with_scores)

	if len_of_rank == 3:
		top_1 = str(ranking_with_scores[0][0])
		top_2 = str(ranking_with_scores[1][0])
		top_3 = str(ranking_with_scores[2][0])
		return (top_1, top_2, top_3)

	elif len_of_rank == 2:
		top_1 = str(ranking_with_scores[0][0])
		top_2 = str(ranking_with_scores[1][0])
		return (top_1, top_2)

	elif len_of_rank == 1:
		top_1 = str(ranking_with_scores[0][0])
		return (top_1)


# get the score of the ranking
def get_score_of_ranking(ranking_with_scores):
	len_of_rank = len(ranking_with_scores)

	if len_of_rank == 3:
		top_1 = str(ranking_with_scores[0][1])
		top_2 = str(ranking_with_scores[1][1])
		top_3 = str(ranking_with_scores[2][1])
		return (top_1, top_2, top_3)

	elif len_of_rank == 2:
		top_1 = str(ranking_with_scores[0][1])
		top_2 = str(ranking_with_scores[1][1])
		return (top_1, top_2)

	elif len_of_rank == 1:
		top_1 = str(ranking_with_scores[0][1])
		return (top_1)


def output_ranking(redis, ranking_with_scores, current_ts):
	# 1. get top 3 from RANKING
	# 2. get post_id, username, score, number_of_commenters
	# 3. create the tuple for output
	temp_store = [current_ts]

	for (post, score) in ranking_with_scores:
		post_id = post.decode('utf-8')
		username = r.find_username(redis, post_id)
		number_of_commenters = r.commenters_number(redis, post_id)
		# add them to a list
		temp_store.extend([post_id, username, int(score), number_of_commenters])

	len_of_rank = len(ranking_with_scores)

	# error	
	if len_of_rank == 1:
		for i in range(2):
			temp_store.extend(['-', '-', '-', '-'])

	elif len_of_rank == 2:
		temp_store.extend(['-', '-', '-', '-'])

	elif len_of_rank == 0:
		for i in range(3):
			temp_store.extend(['-', '-', '-', '-'])

	# cast the list to tuple and return it
	result = tuple(temp_store)
	return result


# output the ranking when all data is received
def end_ranking(redis):
	# RANKING_STORE: get all posts
	# calculate when they expire
	# output ranking until all expire
	results = r.get_top_3_scores(redis)

	# 1. get the ts of the last one
	# 2. calculate the expiration time
	# 3. repeat 1-2 until all expire
	len_of_results = len(results)
	ranking = get_score_of_ranking(results)
	flag = "post"

	if len_of_results == 1:
		post_id = results[0][0].decode("utf-8")
		score = ranking
		ts = find_when_expires(redis, post_id, score, flag)

		output = list()
		output.append(ts)

		for i in range(3):
			output.extend(['-', '-', '-', '-'])

		print(tuple(output))

	elif len_of_results == 2:
		post_id_1 = results[0][0].decode("utf-8")
		post_id_2 = results[1][0].decode("utf-8")
		score_1 = ranking[0]
		score_2 = ranking[1]
		ts = find_when_expires(redis, post_id_2, score_2, flag)
		output = list()
		output.append(ts)

		# calculate the score of #1 post at time ts
		score = find_score(redis, post_id_1, ts, score_1, flag)
		if score <= 0:
			for i in range(3):
				output.extend(['-', '-', '-', '-'])
				
			print(tuple(output))
			return

		else:
			username = r.find_username(redis, post_id_1)
			number_of_commenters = r.commenters_number(redis, post_id_1)
			output.extend([post_id_1, username, score, number_of_commenters, '-', '-', '-', '-', '-', '-', '-', '-'])

		print(tuple(output))

		ts = find_when_expires(redis, post_id_1, score_1, flag)
		output = list()
		output.append(ts)

		for i in range(3):
			output.extend(['-', '-', '-', '-'])

		print(tuple(output))

	elif len_of_results == 3:
		post_id_1 = results[0][0].decode("utf-8")
		post_id_2 = results[1][0].decode("utf-8")
		post_id_3 = results[2][0].decode("utf-8")
		score_1 = ranking[0]
		score_2 = ranking[1]
		score_3 = ranking[2]

		ts = find_when_expires(redis, post_id_3, score_3, flag)
		output = list()
		output.append(ts)

		# calculate the scores of #1, #2 posts at ts
		new_score_1 = find_score(redis, post_id_1, ts, score_1, flag)
		if new_score_1 <= 0: 
			for i in range(3):
				output.extend(['-', '-', '-', '-'])

			print(tuple(output))
			return

		else: 
			username_1 = r.find_username(redis, post_id_1)
			number_of_commenters_1 = r.commenters_number(redis, post_id_1)
			output.extend([post_id_1, username_1, new_score_1, number_of_commenters_1])
			new_score_2 = find_score(redis, post_id_2, ts, score_2, flag)

			if new_score_2 <= 0:
				for i in range(2):
					output.extend(['-', '-', '-', '-'])

				print(tuple(output))

				ts = find_when_expires(redis, post_id_1, score_1, flag)
				output = list()
				output.append(ts)

				for i in range(3):
					output.extend(['-', '-', '-', '-'])

				print(tuple(output))
				return

			else:
				username_2 = r.find_username(redis, post_id_2)
				number_of_commenters_2 = r.commenters_number(redis, post_id_2)
				output.extend([post_id_2, username_2, new_score_2, number_of_commenters_1, '-', '-', '-', '-'])

				print(tuple(output))

		ts = find_when_expires(redis, post_id_2, score_2, flag)
		output = list()
		output.append(ts)

		new_score_1 = find_score(redis, post_id_1, ts, score_1, flag)
		username_1 = r.find_username(redis, post_id_1)
		number_of_commenters_1 = r.commenters_number(redis, post_id_1)
		output.extend([post_id_1, username_1, new_score_1, number_of_commenters_1, '-', '-', '-', '-', '-', '-', '-', '-'])

		print(tuple(output))

		ts = find_when_expires(redis, post_id_1, score_1, flag)
		output = list()
		output.append(ts)

		for i in range(3):
			output.extend(['-', '-', '-', '-'])

		print(tuple(output))


# find the date when a post expires
# return the ts
def find_when_expires(redis, post_id, score, flag):
	score = int(float(score))
	post_last_modified = r.find_last_modified(redis, post_id, flag)
	post_last_modified = time.parse_timestamp(post_last_modified)

	expire_time = find_score_zero_time(redis, post_id, score, flag)

	temp_timestamp = "T".join(time.to_timestamp(expire_time).split(" "))[0:23]

	expire_timestamp = "%s+0000"%(temp_timestamp)

	return expire_timestamp


def find_expire_time(old_time, new_time):
	if isinstance(new_time, datetime.datetime):
		updated_time = old_time.replace(hour=new_time.hour, minute=new_time.minute, microsecond=new_time.microsecond)
		return updated_time

	else:
		len_of_new_time = len(new_time)
		time_data = new_time[len_of_new_time-1]

		days = new_time[0]
		old_time = time.add_days_to_time(old_time, days)
		updated_time = old_time.replace(hour=time_data.hour, minute=time_data.minute, microsecond=time_data.microsecond)
		return updated_time


# find the score of a post at a time
# return score
def find_score(redis, post_id, ts, score, flag):
	post_created = r.find_ts(redis, post_id, flag)
	post_created_time = time.parse_timestamp(post_created)
	post_last_modified = r.find_last_modified(redis, post_id, flag)
	post_last_modified = time.parse_timestamp(post_last_modified)

	target_time = time.parse_timestamp(ts)
	
	count = 0
	is_active = time.is_active(target_time, post_created_time)
	if is_active:
		count += time.get_days_difference(target_time, post_last_modified)

	else: 
		expire_time = time.get_last_active_time(post_created_time)
		count += time.get_days_difference(expire_time, post_last_modified)

	comments = r.get_comment_from_post_set(redis, post_id)
	flag_comment = 'comment'

	for comment in comments:
		comment_id = comment.decode("utf-8")
		comment_ts = r.find_ts(redis, comment_id, flag_comment)
		comment_last_modified = r.find_last_modified(redis, comment_id, flag_comment)
		comment_last_modified_time = time.parse_timestamp(comment_last_modified)
		comment_time = time.parse_timestamp(comment_ts)

		#error here
		is_active = time.is_active(target_time, comment_time)
		if is_active:
			count += time.get_days_difference(target_time, comment_last_modified_time)

		else:
			expire_time = time.get_last_active_time(comment_time)
			count += time.get_days_difference(expire_time, comment_last_modified_time)

	new_score = int(float(score)) - count

	return new_score


# return datetime class 
def find_score_zero_time(redis, post_id, score, flag):
	# get the last modified 
	# get a list of last active, sorted by time, the earliest to lastest, head to tail 
	# calculate each score need decrementing at each last active, last_active - last_modified
	# move to next last_active, new_last_active - pre_last_active
	# output the last_active datetime class, when reach 0 
	last_modified = r.find_last_modified(redis, post_id, flag)
	last_modified_time = time.parse_timestamp(last_modified)

	last_active = get_all_last_active(redis, post_id)

	len_of_last_active = len(last_active)

	i = 0
	pre = last_modified_time
	count = 0
	for each in last_active:
		if time.to_timestamp(pre) == time.to_timestamp(each): 
			i += 1
			continue

		diff = time.get_days_difference(each, pre)
		count += (len_of_last_active - i) * diff
		i += 1
		pre = each

		if count > score:
			index = len_of_last_active - 1 - ((count - score) % len_of_last_active)
			return last_active[index]

		elif count == score:
			return last_active[len_of_last_active - 1]


# return list of datetime class
def get_all_last_active(redis, post_id):
	# get a list of last active, sorted by time, the earliest to lastest, head to tail
	last_active = list()

	post_ts = time.parse_timestamp(r.find_ts(redis, post_id, "post"))
	post_last_active = time.get_last_active_time(post_ts)
	last_active.append(post_last_active)

	comments = r.get_comment_from_post_set(redis, post_id)

	for c in comments:
		comment = c.decode("utf-8")
		comment_ts = time.parse_timestamp(r.find_ts(redis, comment, "comment"))
		comment_last_active = time.get_last_active_time(comment_ts)

		i = 0
		for item in last_active:
			is_earlier = time.is_earlier(comment_last_active, item)
			if is_earlier:
				last_active.insert(i, comment_last_active)
				break

			else:
				i += 1
				if i == len(last_active):
					last_active.append(comment_last_active)
					break

	return last_active


if __name__ == '__main__':
	pass