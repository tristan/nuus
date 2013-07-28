from datetime import datetime, timedelta
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
    (re.compile('^\d{2} [\w]{3} \d{4} \d{2}:\d{2}:\d{2} .+$'), 
     lambda x: dt2ut(datetime.strptime(x, '%d %b %Y %H:%M:%S %Z'))),
    # XXX, ?0 XXX 0000 00:00:00 +0000
    (re.compile('^[\w]{3}, \d{1,2} [\w]{3} \d{4} \d{2}:\d{2}:\d{2} [+-]\d{4}'), 
     lambda x: dt2ut(datetime.strptime(x[:-6], '%a, %d %b %Y %H:%M:%S') - timedelta(hours=int(x[-5:])/100))),
    # XXX, 0, XXX 0000 00:00:00 XXX
    (re.compile('^[\w]{3}, \d{2} [\w]{3} \d{4} \d{2}:\d{2}:\d{2} \w{3}$'),
     lambda x: dt2ut(datetime.strptime(x, '%a, %d %b %Y %H:%M:%S %Z'))),
]

def parse_date(datestr):
    """Checks common date formats in an attempt to convert a string into a date (returns unixtime)"""
    for p, fn in date_formats:
        if p.match(datestr):
            return fn(datestr)
    raise ValueError('Unable to handle date format: "%s"' % datestr)        
