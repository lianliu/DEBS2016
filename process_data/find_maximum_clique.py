import time


def find_maximum_clique(comment):
    start_time = time.time()
    print 'Finding maximum clique of %s consumes ---%s seconds ---' % (comment['comment_id'], time.time() - start_time)
