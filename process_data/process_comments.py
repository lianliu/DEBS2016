import time
import get_node
from py2neo import Node, Relationship


def establish(root, comment_id, graph):
    # Check if the post is active.
    # If yes, establish the relationship;
    # if no, do nothing
    if root is not None:
        comment = get_node.get_node(comment_id, graph, "Comment")
        rel = Relationship.cast(comment, 'BELONGS_TO', root)
        graph.create_unique(rel)


# Find the root post
# and update the score.
def update_score(post):
    post['score'] += 10
    post.push()


# Find the root post
# and update the number of commentators by 1
def update_commentators(post):
    post['number_of_commentators'] += 1
    post.push()


def process_comments(address, graph):
    start_time = time.time()
    d = {}

    with open(address) as comments:
        for comment in comments:
            c = comment.split('|')

            d['time_stamp'] = c[0]
            d['comment_id'] = c[1]

            root_post = c[6].strip()

            if root_post:
                d['root'] = root_post
            else:
                comment_replied = str(c[5])
                parent_comment = get_node.get_node(comment_replied, graph, "Comment")
                d['root'] = str(parent_comment['root'])

            d['score'] = 10
            d['size'] = 0

            comment_node = Node.cast(d)
            comment_node.labels.add('Comment')

            graph.create(comment_node)

            root = get_node.get_node(d['root'], graph, "Active")

            # Establish the relationship with post
            establish(root, d['comment_id'], graph)

            # Update score
            update_score(root)

            # Update the number of commentators
            update_commentators(root)

    print 'Processing comments consumes ---%s seconds ---' % (time.time() - start_time)
