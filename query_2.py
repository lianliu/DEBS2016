import subscriber_2_single as subscriber_2
import time


if __name__ == "__main__":
	d = 7200

	k = 2

	start_time = time.time()

	print("Start.")

	subscriber_2.main(d, k)

	print("Completed. %s"%(time.time()-start_time))