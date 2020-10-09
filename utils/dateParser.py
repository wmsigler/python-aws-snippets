import datetime
import time


def get_date_from_iso(date_str, format='%Y-%m-%dT%H:%M:%S%z'):
    """
    :param date_str: '2020-01-15T00:00:00Z'
    :return: 2020-01-15
    """
    date = datetime.datetime.strptime(date_str, format)
    return date.date()

def current_milli_time():
    return int(round(time.time() * 1000))
