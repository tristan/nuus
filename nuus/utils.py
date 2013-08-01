import copy
from datetime import datetime, timedelta
from itertools import groupby
from functools import wraps
from operator import itemgetter
import re
import time

class rkey(object):
    """This is a nasty little hack that i'm only allowing because i'm jetlagged!"""
    def __new__(cls, *args):
        return ':'.join(map(str, args))
    
    @classmethod
    def split(cls, key):
        return key.split(':')
        
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

class Range(object):
    def __contains__(self, x):
        for r in self.ranges:
            if r[0] <= x and r[1] >= x:
                return True
        return False
    def __init__(self, *args):
        self.ranges = self._ranges(args)
    def __repr__(self):
        return self.ranges.__repr__()
    def _combine_ranges(self, range1, range2):
        ranges = sorted(range1 + range2)
        i = 1
        while i < len(ranges):
            if ranges[i][0] <= ranges[i-1][1]:
                if ranges[i][1] > ranges[i-1][1]:
                    ranges[i-1] = (ranges[i-1][0], ranges[i][1])
                del ranges[i]
            elif ranges[i][0] == ranges[i-1][1]+1:
                ranges[i-1] = (ranges[i-1][0], ranges[i][1])
                del ranges[i]
            else:
                i+= 1
        return ranges
    def _ranges(self, items):
        ranges = []
        for k, g in groupby(enumerate(items), lambda (i,x):i-x):
            group = map(itemgetter(1), g)
            ranges.append((group[0], group[-1]))
        return ranges
    def add(self, *args):
        self.ranges = self._combine_ranges(self.ranges, self._ranges(args))
    def missing(self, start=None, end=None):
        """Returns the ranges missing between `start` and `end`"""
        ex = []
        if start is not None:
            ex.append((start-1,start-1))
        if end is not None:
            ex.append((end+1,end+1))
        rgs = self._combine_ranges(self.ranges, ex)
        inv = []
        i = 1
        while i < len(rgs):
            inv.append((rgs[i-1][1]+1, rgs[i][0]-1))
            i+=1
        return self._combine_ranges(inv, [])
