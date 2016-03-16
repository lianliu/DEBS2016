import time
import get_node
import find_maximum_clique as clique


def update_size(comment, max_size):
    comment['size'] = max_size
    comment.push()


def process_likes(address, graph):
    start_time = time.time()
    with open(address) as likes:
        for like in likes:
            l = like.split('|')

            user_id = str(l[0])
            comment_id = str(l[1])

            comment = get_node.get_node(comment_id, graph, 'Comment')
            max_size = clique.find_maximum_clique(comment)
            update_size(comment, max_size)

    print 'Processing likes consumes ---%s seconds ---' % (time.time() - start_time)
