import os
import sys
base_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(base_dir, '..')
sys.path.append(os.path.abspath(parent_dir))

import process_data.time_transforms as time
import process_data.redis_operations as r
import constants


# TODO rewrite to pipeline manner
def check_comments(active_store, redis, post_id, current_ts):
	amount = 0
	comments = r.get_comment_from_post_set(active_store, post_id)
	flag = "comment"

	if comments:
		for comment in comments:
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
			#11. update score in RANKING
		
			ts = r.find_ts(redis, comment, flag)

			current_time = time.parse_timestamp(current_ts)
			comment_time = time.parse_timestamp(ts)

			last_modified = r.find_last_modified(redis, comment, flag)
			last_modified_time = time.parse_timestamp(last_modified)

			is_active = time.is_active(current_time, comment_time)

			if is_active:
				diff = time.time_diff(current_time, last_modified_time)
				is_day_pass = time.is_day_pass(current_time, last_modified_time)

				if is_day_pass:
					amount += diff.days

					new_time = time.update_time(last_modified_time, current_time)
					new_timestamp = time.to_timestamp(new_time)
					r.update_ts(redis, comment, new_timestamp, flag)

			else:
				last_active_time = time.get_last_active_time(comment_time)
				diff = time.time_diff(last_active_time, last_modified_time)

				amount += diff.days
	
			r.decrease_score(active_store, post_id, amount)


#TODO rewrite to pipeline manner
def check_posts(redis, current_ts):
	amount = 0
	posts = r.get_posts_from_ranking(redis)
	flag = "post"

	pipeline = redis.pipeline()

	for post_id in posts:
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

		last_modified = r.find_last_modified(redis, post, flag)

		if current_ts != last_modified:
			last_active_time = time.parse_timestamp(last_modified)
			score = r.get_score(redis, post)

			ts = r.find_ts(redis, post, flag)

			current_time = time.parse_timestamp(current_ts)

			post_time = time.parse_timestamp(ts)

			diff = time.time_diff(current_time, post_time)
			days = diff.days
			if days >= 1:
				amount = days

				if amount >= score:
					r.clear_post(pipeline, post)

				else: 
					r.decrease_score(redis, post, amount)
				
					new_time = time.update_time(last_active_time, current_time)
					new_timestamp = time.to_timestamp(new_time)
					r.update_ts(redis, post, new_timestamp, flag)

	pipeline.execute()


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
	len_of_rank = len(ranking_with_scores)

	for (post, score) in ranking_with_scores:
		post_id = post.decode('utf-8')
		username = r.find_username(redis, post_id)
		number_of_commenters = r.commenters_number(redis, post_id)
		# add them to a list
		temp_store.extend([post_id, username, int(score), number_of_commenters])

	if len_of_rank == 1:
		for i in range(2):
			temp_store.extend(['-', '-', '-', '-'])

	elif len_of_rank == 2:
		temp_store.extend(['-', '-', '-', '-'])

	# cast the list to tuple and return it
	result = tuple(temp_store)
	return result


if __name__ == '__main__':
	pass