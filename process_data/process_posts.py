import time
from py2neo import Node


def process_posts(address, graph):
    start_time = time.time()
    d = {}

    with open(address) as posts:
        for post in posts:
            p = post.split('|')

            d['time_stamp'] = p[0]
            d['post_id'] = p[1]
            d['user_id'] = p[2]

            user_name = p[4]
            d['user'] = user_name.strip()

            # The initial score of a post, 10
            d['score'] = 10

            # The initial number of commentators, 0
            d['number_of_commentators'] = 0

            post_node = Node.cast(d)
            post_node.labels.add('Active')
            graph.create(post_node)

    print 'Processing posts consumes ---%s seconds ---' % (time.time() - start_time)
