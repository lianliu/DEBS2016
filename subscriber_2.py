import zmq
import redis
import constants
from process_data import redis_operations 
from process_data import time_transforms as time
from process_data import util
import time


def main(d, k):
    # open ZMQ PULL socket to receive events
    context= zmq.Context()
    subscriber= context.socket(zmq.PULL)
    subscriber.set_hwm(constants.HWM)
    subscriber.connect(constants.IPC_ADDRESS)

    # connect redis servers
    # maybe need a couple of redis stores for my purpose
    print("Start Redis 6379.")
    pool_1 = redis.ConnectionPool(host='localhost', port=6379, db=0)
    active_store = redis.Redis(connection_pool=pool_1)

    # create the window store here 
    # a list, name: WINDOW, key: comment_id

    try:
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
                

            elif event['event_type'] == 'like':
                pass

            elif event['event_type'] == 'fship':
                pass

            if event['event_type'] != 'post':
                #do the checking work here
                pass

    except KeyboardInterrupt:
        # clean-up
        subscriber.close()
        context.term()


if __name__ == "__main__":
    main()