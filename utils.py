import pprint
import time
from functools import wraps

from datetime import datetime

import pytz
from pytz import timezone
from termcolor import colored

def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__!r} executed in {(end_time - start_time):.4f}s")
        return result
    return wrapper

class Utils:
    @staticmethod
    def get_formatted_pst():
        date_format = '%m/%d/%Y %H:%M:%S %Z'
        date = datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific'))
        return date.strftime(date_format)

    '''
    Prints with nicely-formatted string and color.
    '''
    @staticmethod
    def print(*args, color=None):
        formatted_string = ' '.join(pprint.pformat(arg) if not isinstance(arg, str) else arg for arg in args)
        if color:
            formatted_string = colored(formatted_string, color)
        print('[{pst_time}] {formatted_string}'.format(pst_time=Utils.get_formatted_pst(), formatted_string=formatted_string))
