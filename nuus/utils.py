import copy
from datetime import datetime, timedelta
from itertools import groupby
from functools import wraps
from operator import itemgetter
import re
import time
        
def dt2ut(dt):
    """datetime to unixtime"""
    return int(time.mktime(dt.timetuple()))

# (regex to match a date, function to parse the date to unix time)
date_formats = [
    # unixtime
    (re.compile('^\d+(?:\.\d+)?$'), int),
    # 00 XXX 0000 00:00:00 XXX
    (re.compile('^\d{2}\s+[\w]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\w{3}$'), 
     lambda x: dt2ut(datetime.strptime(x[0], '%d %b %Y %H:%M:%S %Z'))),
    # Fri, 17 May 2013 15:14:04 +0000 (UTC)
    (re.compile('^(?:[\w]{3},\s+)?(\d{1,2}\s+[\w]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2})\s+([+-]?\d{4})(?:\s+\(\w{3}\))?$'), 
     lambda x: dt2ut(datetime.strptime(x[0], '%d %b %Y %H:%M:%S') - timedelta(hours=int(x[1])/100))),
    # XXX, ?0, XXX 0000 00:00:00 XXX
    (re.compile('^[\w]{3},\s+\d{1,2}\s+[\w]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+\w{3}$'),
     lambda x: dt2ut(datetime.strptime(x[0], '%a, %d %b %Y %H:%M:%S %Z'))),
    # turn empty strings into 0!
    (re.compile('^\s*$'), lambda x: 0),
]

def parse_date(datestr):
    """Checks common date formats in an attempt to convert a string into a date (returns unixtime)"""
    for p, fn in date_formats:
        m = p.match(datestr)
        if m:
            return fn(m.groups() or (datestr,))
    raise ValueError('Unable to handle date format: "%s"' % datestr)        

def date_deltas(offset):
    delta_s = offset % 60
    offset /= 60
    delta_m = offset % 60
    offset /= 60
    delta_h = offset % 24
    offset /= 24
    delta_d = offset
    return (delta_d, delta_h, delta_m, delta_s)

def humanize_date_difference(since, until=None, offset=None):
    if until is None:
        until = time.time()
    until -= since
    delta_d, delta_h, delta_m, delta_s = date_deltas(until)

    if delta_d > 0:
        return "%d days ago" % delta_d
    if delta_h > 0:
        return "%dh%dm ago" % (delta_h, delta_m)
    if delta_m > 0:
        return "%dm%ds ago" % (delta_m, delta_s)
    else:
        return "%ds ago" % delta_s

def time_remaining(start_time, things_completed, things_remainings, now=None):
    if now is None:
        now = time.time()
    time_taken = now - start_time
    time_per_thing = time_taken / things_completed
    offset = time_per_thing * things_remainings
    delta_d, delta_h, delta_m, delta_s = date_deltas(offset)

    rval = ''
    if delta_d > 0:
        rval += "%d days " % delta_d
    if delta_h > 0:
        rval += "%d hours " % delta_h
    if delta_m > 0:
        rval += "%d mins " % delta_m
    if delta_s > 0:
        rval += "%d secs " % delta_s
    
    return rval[:-1]
    

def swallow(exception):
    """Wraps function in a try except and silently swallows the specified exception
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except exception:
                pass
        return wrapper
    return decorator

