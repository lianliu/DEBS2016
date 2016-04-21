import zmq
import redis
import constants
from process_data import redis_operations 
from process_data import time_transforms as time
from process_data import query_1 as util
import time


def main():
    # open ZMQ PULL socket to receive events
    context= zmq.Context()
    subscriber= context.socket(zmq.PULL)
    subscriber.set_hwm(constants.HWM)
    subscriber.connect(constants.IPC_ADDRESS)

    # connect redis servers
    # redis store for Post:p_id
    print("Start Redis 6379.")
    pool_1 = redis.ConnectionPool(host='localhost', port=6379, db=0)
    active_store = redis.Redis(connection_pool=pool_1)

    # redis store for Comment:c_id
    print("Start Redis 6389.")
    pool_2 = redis.ConnectionPool(host='localhost', port=6389, db=0)
    c_store = redis.Redis(connection_pool=pool_2)

    # redis store for P:p_id
    print("Start Redis 6399.")
    pool_3 = redis.ConnectionPool(host='localhost', port=6399, db=0)
    p_store = redis.Redis(connection_pool=pool_3)

    # redis store for Commenters:p_id
    print("Start Redis 6409.")
    pool_4 = redis.ConnectionPool(host='localhost', port=6409, db=0)
    commenter_store = redis.Redis(connection_pool=pool_4)

    # redis store for RANKING and RANKING_STORE
    print("Start Redis 6419.")
    pool_5 = redis.ConnectionPool(host='localhost', port=6419, db=0)
    ranking_store = redis.Redis(connection_pool=pool_5)

    # TODO active_store => ranking_store
    redis_operations.init_ranking_store(active_store)
    current_ts = ''

    try:
        while True:
            event = subscriber.recv_json()
            post_id_comment = ""
            if not event:
                #util.end_ranking(active_store)
                break

            # ADD EVENT PROCESSING HERE
            if event['event_type'] == 'post':
                # 1. add it to post hash @active_store
                # 2. add it to ranking sorted set @active_store

                post_id = event['post_id']
                username = event['user']
                timestamp = event['ts']
                current_ts = timestamp

                redis_operations.create_post_hash(active_store, post_id, username, timestamp)

                # TODO active_store => ranking_store
                redis_operations.add_to_ranking(active_store, post_id)

            elif event['event_type'] == 'comment':
                # 1. add it comment hash @r
                # 2. check if its belonging post is active, yes and add it to post set @active_store

                timestamp = event['ts']
                current_ts = timestamp
                post_id = event['post_commented']
                comment_id = event['comment_id']
                user_id = event['user_id']
                post_id_comment = post_id

                # TODO active_store => commenter_store
                redis_operations.add_commenter_to_set(active_store, post_id, user_id)

                # -1 or null
                if not post_id:
                    parent_comment = event['comment_replied']
                    # TODO active_store => c_store
                    post_id = redis_operations.find_post(active_store, parent_comment)
                    post_id_comment = post_id

                # TODO active_store => c_store
                redis_operations.create_comment_hash(active_store, comment_id, post_id, timestamp)

                # TODO active_store => ranking_store
                is_post_active = redis_operations.is_post_active(active_store, post_id)

                if is_post_active:
                    # TODO active_store => p_store
                    redis_operations.add_comment_to_post(active_store, post_id, comment_id)
                    # TODO active_store => ranking_store
                    redis_operations.increase_score(active_store, post_id)

            if (event['event_type'] == 'comment' or event['event_type'] == 'post'):
                # 1. do the TTL checking here
                # 2. get the ranking here

                # a. get the new ranking
                # b. add it to ranking_store to check if is new ranking
                # c. output new ranking 

                post_id = "" 

                if event['event_type'] == 'comment':
                    post_id = post_id_comment
                elif event['event_type'] == 'post':
                    post_id = event['post_id']   

                # TODO active_store => none
                util.check_posts(active_store, current_ts)

                # TODO active_store => active_store, c_store
                util.check_comments(active_store, current_ts)

                # TODO active_store => ranking_store
                ranking_with_scores = redis_operations.get_top_3_scores(active_store)

                # TODO active_store => ranking_store
                new_ranking = util.get_new_ranking(ranking_with_scores)

                is_diff = redis_operations.add_to_ranking_store(active_store, new_ranking)
                if is_diff == 1:
                    # 1. output new ranking
                    # 2. initialize ranking store

                    # TODO active_store => active_store, ranking_store, commenter_store
                    output = util.output_ranking(active_store, ranking_with_scores, current_ts)

                    print(output)

                    # TODO active_store => ranking_store
                    redis_operations.renew_ranking_store(active_store, new_ranking)

    except KeyboardInterrupt:
        # clean-up
        subscriber.close()
        context.term()


if __name__ == "__main__":
    main()
