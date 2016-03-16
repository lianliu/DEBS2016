import time


def process_friendships(address, graph):
    start_time = time.time()
    with open(address) as friendships:
        for friendship in friendships:
            f = friendship.split('|')
            user_1 = f[1]
            user_2 = f[2]

            print user_1
            print user_2
    print 'Processing friendships consumes ---%s seconds ---' % (time.time() - start_time)