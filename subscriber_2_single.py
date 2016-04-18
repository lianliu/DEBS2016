import zmq
import redis
import constants
from process_data import redis_operations
from process_data import redis_operations_2 as r 
from process_data import time_transforms as time
from process_data import query_2 as util


def main(d, k):
    if d <= 0 and d > 43200:
        return

    # open ZMQ PULL socket to receive events
    context= zmq.Context()
    subscriber= context.socket(zmq.PULL)
    subscriber.set_hwm(constants.HWM)
    subscriber.connect(constants.IPC_ADDRESS)

    # connect redis servers
    # maybe need a couple of redis stores for my purpose
    print("Start Redis 6379.")
    pool_1 = redis.ConnectionPool(host='localhost', port=6379, db=0)
    store = redis.Redis(connection_pool=pool_1)

    # create the RANKING_STORE 
    # a set

    duration = d

    try:
        # 1. test the size of time window at different time 
        # 2. test the size of likers of each comment in the time window at different time

        while True:
            event = subscriber.recv_json()
            if not event:
                break

            # ADD EVENT PROCESSING HERE
            if event['event_type'] == 'comment':
                # only maintain comments within the time window
                # if out of bound, deleted
                # Comment:comment_id => ts, comment, user_id
                ts = event['ts']
                content = event['comment']
                user_id = event['user_id']
                comment_id = event['comment_id']

                # save the comment hash to store
                # save the c_id:ts to window store, check if list exists, no create one

                r.create_comment_hash(store, comment_id, user_id, content, ts)

                # add to the head of the list
                r.add_comment_to_window(store, ts, comment_id)

            elif event['event_type'] == 'like':
                # check if comment liked is in the list WINDOW
                # yes, save to like:c_id
                # no, do nothing

                '''
                ts = event['ts']
                comment_id = event['comment_id']
                user_id = event['user_id']
                '''
                pass

            elif event['event_type'] == 'fship':
                '''
                ts = event['ts']
                user_id_1 = event['user_id_1']
                user_id_2 = event['user_id_2']
                '''
                pass

            if event['event_type'] != 'post':
                # do the checking work here
                # 1. first clear obsolete comments
                # 2. find maximum clique of each comment
                # 3. output ranking

                current_ts = event['ts']

                util.check_list(store, current_ts, duration)
                print(current_ts, r.check_window_len(store))
                # ranking = util.output_rank(redis, current_ts)
                # print(ranking)


    except KeyboardInterrupt:
        # clean-up
        subscriber.close()
        context.term()


if __name__ == "__main__":
    main()