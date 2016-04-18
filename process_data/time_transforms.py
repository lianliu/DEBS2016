from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from datetime import date
import time


# take 0.0001+s
def parse_timestamp(ts):
	time = parse(ts)

	# return type: datetime.datetime
	return time


def to_timestamp(time):
	#return type: str
	return str(time)


def is_earlier(time_1, time_2):
	if time_1 < time_2:
		return True

	else:
		return False


# return True, when time_1 is earlier in hour, minute, and second
def compare_hms(time_1, time_2):
	h1 = time_1.hour
	h2 = time_2.hour
	m1 = time_1.minute
	m2 = time_2.minute
	s1 = time_1.second
	s2 = time_2.second
	ms1 = time_1.microsecond
	ms2 = time_2.microsecond

	t1 = (1 / 1000 * ms1) + s1 + 60 * (m1 + 60 * h1)
	t2 = (1 / 1000 * ms2) + s2 + 60 * (m2 + 60 * h2)

	if t1 >= t2:
		return False

	else:
		return True


def get_sec_diff(time_1, time_2):
	diff = time_1 - time_2

	seconds = diff / timedelta(seconds=1)

	seconds = abs(seconds)

	return seconds


def get_days_difference(new_time, old_time):
	d_1 = date(new_time.year, new_time.month, new_time.day)
	d_2 = date(old_time.year, old_time.month, old_time.day)

	diff = d_1 - d_2
	days = diff.days

	if compare_hms(new_time, old_time):
		days -= 1
		
	return days


def is_active(new_time, old_time):
	diff = get_days_difference(new_time, old_time)

	#return type: boolean
	if diff >= 10:
		return False
	else:
		return True


def get_last_active_time(time):
	last_active_time = time + timedelta(days=10)

	return last_active_time


def add_days_to_time(old_time, days):
	new_time = old_time + timedelta(days=days)
	return new_time


def update_time(old_time, new_time):
	# get the new year, month, and day
	new_year = get_year(new_time)
	new_month = get_month(new_time)
	new_day = get_day(new_time)

	updated_time = old_time.replace(year=new_year, month=new_month, day=new_day)

	return updated_time


def get_year(new_time):
	new_year = new_time.year

	# return type: int
	return new_year


def get_month(new_time):
	new_month = new_time.month

	# return type: int
	return new_month


def get_day(new_time):
	new_day = new_time.day

	# return type: int
	return new_day


if __name__ == '__main__':
	ts_1 = "2010-02-17T07:29:53.261+0000"
	ts_2 = "2010-02-17T00:41:03.042+0000"
	ts_3 = "2010-02-13T14:18:54.127+0000"
	ts_4 = "2010-03-04T17:08:46.321+0000"
	ts_5 = "2010-12-29T17:08:46.321+0000"
	ts_6 = "2010-12-30T17:08:46.321+0000"

	t_1 = parse_timestamp(ts_1)
	t_2 = parse_timestamp(ts_2)
	t_3 = parse_timestamp(ts_3)
	t_4 = parse_timestamp(ts_4)
	t_5 = parse_timestamp(ts_5)
	t_6 = parse_timestamp(ts_6)

	print(get_sec_diff(t_5, t_6))
	'''
	print(is_earlier(t_5, t_6))

	s = time.time()
	print(get_days_difference(t_6, t_5))
	print(time.time()-s)
	
	ts = ts_1.split("T")
	ts_2 = ts_3.split("T")

	t = parse_timestamp(ts[1])
	t2 = parse_timestamp(ts_2[1])
	print(t.hour)

	s = time.time()
	t_1 = parse_timestamp(ts_1)
	print(time.time()-s)

	print(type(time_diff(t_1, t_2)))

	print(t_1)
	print(type(t_1))

	print(is_day_passed(t_2, t_1))
	print(is_day_passed(t_1, t_3))

	print(type(to_timestamp(t_1)))

	print(update_time(t_1, t_4))
	

	time_1 = get_last_active_time(t_1)
	time_2 = get_last_active_time(t_5)
	time_3 = get_last_active_time(t_6)
	print(time_1)
	print(time_2)
	print(time_3)
	'''