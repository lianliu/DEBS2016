# ZMQ specific config 
IPC_ADDRESS='ipc://firehose'
HWM=100000

# Data file paths
COMMENTS_FILE_PATH='./data/comments.dat'
FSHIPS_FILE_PATH='./data/friendships.dat'
LIKES_FILE_PATH='./data/likes.dat'
POSTS_FILE_PATH='./data/posts.dat'

# Event types
EVENT_TYPE_KEY='event_type'
COMMENT_KEY='comment'
FSHIP_KEY='fship'
LIKE_KEY='like'
POST_KEY='post'

# Data schemas
COMMENT_SCHEMA=['ts','comment_id','user_id','comment','user','comment_replied','post_commented']
FSHIP_SCHEMA=['ts','user_id_1','user_id_2']
LIKE_SCHEMA=['ts','user_id','comment_id']
POST_SCHEMA=['ts','post_id','user_id','post','user']

# Key names
RANKING_STORE = 'RANKING_STORE'
RANKING = 'RANKING'
WINDOW = 'WINDOW'
