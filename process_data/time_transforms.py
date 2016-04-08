from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import timedelta

def parse_timestamp(ts):
	time = parse(ts)

	# return type: datetime.datetime
	return time


def to_timestamp(time):
	#return type: str
	return str(time)


def time_diff(new_time, old_time):
	diff = relativedelta(new_time, old_time)
	return diff


def is_day_passed(new_time, old_time):
	diff = time_diff(new_time, old_time)

	#return type: boolean
	if diff.days > 0:
		return True
	else:
		return False


def is_active(new_time, old_time):
	diff = time_diff(new_time, old_time)

	#return type: boolean
	if diff.days >= 10:
		return False
	else:
		return True


def get_last_active_time(time):
	last_active_time = time + timedelta(days=10)

	return last_active_time


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
	ts_5 = "2010-03-24T17:08:46.321+0000"
	ts_6 = "2010-12-30T17:08:46.321+0000"

	t_1 = parse_timestamp(ts_1)
	t_2 = parse_timestamp(ts_2)
	t_3 = parse_timestamp(ts_3)
	t_4 = parse_timestamp(ts_4)
	t_5 = parse_timestamp(ts_5)
	t_6 = parse_timestamp(ts_6)
	'''
	print(t_1)
	print(type(t_1))

	print(is_day_passed(t_2, t_1))
	print(is_day_passed(t_1, t_3))

	print(type(to_timestamp(t_1)))

	print(update_time(t_1, t_4))
	'''

	time_1 = get_last_active_time(t_1)
	time_2 = get_last_active_time(t_5)
	time_3 = get_last_active_time(t_6)
	print(time_1)
	print(time_2)
	print(time_3)