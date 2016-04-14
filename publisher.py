import zmq
from dateutil.parser import parse
from constants import *

event_construction_dict={
  COMMENT_KEY:(lambda comment_str: \
    dict(zip(COMMENT_SCHEMA,comment_str.rstrip().split('|')))),
  FSHIP_KEY:(lambda fship_str: \
    dict(zip(FSHIP_SCHEMA,fship_str.rstrip().split('|')))),
  LIKE_KEY:(lambda like_str: \
    dict(zip(LIKE_SCHEMA,like_str.rstrip().split('|')))),
  POST_KEY:(lambda post_str: \
    dict(zip(POST_SCHEMA,post_str.rstrip().split('|'))))
}

def get_event(k,v):
    return event_construction_dict[k](v)
    
#Converts read lines into event dictionaries
def get_events_dict(lines):
    return {k:get_event(k,v) for k,v in lines.items() if v!=""}

#Extracts timestamps from events dictionary
def get_ts_dict(events):
    return {k:parse(v['ts']) for k,v in events.items()}

def main():
    #open ZMQ PUSH socket to send events
    context=zmq.Context()
    publisher=context.socket(zmq.PUSH)
    publisher.set_hwm(HWM)
    publisher.bind(IPC_ADDRESS)

    with open(COMMENTS_FILE_PATH) as comments,\
      open(FSHIPS_FILE_PATH) as fships,\
      open(LIKES_FILE_PATH) as likes,\
      open(POSTS_FILE_PATH) as posts:

        file_descriptors={COMMENT_KEY:comments,
          FSHIP_KEY: fships,
          LIKE_KEY: likes,
          POST_KEY: posts}

        #Read first lines from each input data file
        lines={COMMENT_KEY:comments.readline(),
          FSHIP_KEY:fships.readline(),
          LIKE_KEY:likes.readline(),
          POST_KEY:posts.readline()} 

        while any(line!="" for line in lines.values()):
            #Convert lines into specific events
            events=get_events_dict(lines)

            #Find the event with earliest timestamp
            key,min_ts=min(get_ts_dict(events).items(), key=lambda x: x[1])
            
            #Read next line 
            lines[key]=file_descriptors[key].readline() 
            
            #Add event_type information to the event to be sent
            event_to_send=events[key]
            event_to_send[EVENT_TYPE_KEY]=key
            #Send event with earliest timestamp
            publisher.send_json(event_to_send)

        #Perform clean-up
        publisher.send_json(None)      
        publisher.close()
        context.term()

if __name__=="__main__":
    main()
