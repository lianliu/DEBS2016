"""
    At this stage, we don't take the time into consideration.
    So the posts are assumed to be active at all time.
    Because of this, the update_score function is partial and not complete.
    And this will definitely affect the final outcome of the ranking.

    I also don't parallelize the data reading at this stage.
    The multiprocessing module in Python would be helpful for our purpose.
"""


from py2neo import Graph, authenticate
from process_data import process_posts as posts
from process_data import process_comments as comments
from process_data import process_likes as likes
from process_data import process_friendships as friendships
from query_1 import query_1
from query_2 import query_2
import time


if __name__ == '__main__':
    graph = Graph()
    authenticate("localhost:7474", "neo4j", "123")
    graph.delete_all()

    start_time = time.time()
    print "Start processing..."

    posts_address = './test_data/posts.dat'
    comments_address = './test_data/comments.dat'
    likes_address = './test_data/likes.dat'
    friendships_address = './test_data/friendships.dat'

    posts.process_posts(posts_address, graph)
    comments.process_comments(comments_address, graph)
    # likes.process_likes(likes_address, graph)
    # friendships.process_friendships(friendships_address, graph)

    query_1.ranking(3, graph)
    # query_2.ranking(3, 30000000, graph)
    print 'Finish... It takes ---%s seconds ---' % (time.time() - start_time)
