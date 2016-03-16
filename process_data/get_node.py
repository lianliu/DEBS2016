def get_node(node_id, graph, label):
    """
    :param node_id: str
    :param graph: Graph
    :param label: str
    """

    node_type = 'post_id'
    if label == 'Comment':
        node_type = 'comment_id'

    node = graph.merge_one(label, node_type, node_id)
    return node
