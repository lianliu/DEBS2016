import subscriber
import time
import os


if __name__ == "__main__":
	start_time = time.time()

	print("Start.")
	
	subscriber.main()

	print("Completed. %s"%(time.time()-start_time))
