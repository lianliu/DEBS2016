import zmq
from constants import *
from dateutil.parser import parse

def main():
    #open ZMQ PULL socket to receive events
    context= zmq.Context()
    subscriber= context.socket(zmq.PULL)
    subscriber.set_hwm(HWM)
    subscriber.connect(IPC_ADDRESS)
    try:
        while True:
            event = subscriber.recv_json()
            if not event:
                break
            #TODO ADD EVENT PROCESSING HERE
            print("ts: %s \t event_type: %s"%(parse(event['ts']),event['event_type']))
    except KeyboardInterrupt:
        #clean-up
        subscriber.close()
        context.term()

if __name__== "__main__":
   main()
