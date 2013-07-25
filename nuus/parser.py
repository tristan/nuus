import cgi
import cPickle as pickle
from datetime import datetime, timedelta
import gzip
import md5
import os
import re
import time

patterns = [
    # #a.b.moovee typical release format
    '^\[[0-9]+\]-\[FULL\]-\[#[^\]]+\]-\[ (?P<release_name>[^ ]+) \]-\[(?P<release_part>[0-9]+)/(?P<release_total>[0-9]+)\] - "(?P<file_name>[^"]+)" yEnc \((?P<file_part>[0-9]+)/(?P<file_total>[0-9]+)\)$'
]
patterns = map(re.compile, patterns)

class Segment(object):
    def __eq__(self, s2):
        if s2 is None or not isinstance(s2, Segment):
            return False
        return self.number == s2.number and self.bytes == s2.bytes and self.id == s2.id
    def __init__(self, number, bytes, id):
        self.number = number
        self.bytes = bytes
        self.id = id
    def __repr__(self):
        return "%s, %s, %s" % (self.number, self.bytes, self.id)
    def nzb(self):
        return '<segment bytes="%s" number="%s">%s</segment>' % (
            self.bytes, self.number, self.id)
    def todict(self):
        return dict(number=self.number,bytes=self.bytes,id=self.id)

class File(object):
    def __init__(self, key, name, total_parts, poster, date=None, subject=None):
        self.key = key
        self.name = name
        self.total_parts = total_parts
        self.poster = poster
        self.date = date
        self.subject = subject
        self.groups = set()
        self.segments = [None] * total_parts
    def __repr__(self):
        return "FILE<%s,%s,%s,%s,%s/%s>" % (
            self.name, self.subject, self.date, self.poster, len(filter(None, self.segments)), self.total_parts)
    def complete(self):
        return all(self.segments)
    def insert_part(self, group, article_id, part, size, subject=None, date=None):
        # TODO: the 'bytes' (i.e. size) value i get seems to be different from what nzbindex.nl gives
        # sabnzbd doesn't seem to care though, so whatever!
        if part > self.total_parts:
            raise ValueError("Got part greater than max part number")
        s = Segment(part, size, article_id)
        if self.segments[part-1] is not None:
            if s != self.segments[part-1]:
                pass # NOTE: nzbindex.nl seems to just take the first entry, so that's what i'm gonna do!
        else:
            self.segments[part-1] = s
            self.groups.add(group)
            # set subject to the same values as found in the first article for the file
            if part == 1:
                if subject is not None:
                    self.subject = subject
                if date is not None:
                    self.date = date
    def nzb(self):
        if not self.complete():
            raise ValueError("cannot create nzb for incomplete file")
        nzb = '<file poster="%s" date="%s" subject="%s">\n' % (
            cgi.escape(self.poster, quote=True), self.date, cgi.escape(self.subject, quote=True))
        nzb += '<groups>\n'
        for g in self.groups:
            nzb += '<group>%s</group>\n' % g
        nzb += '</groups>\n<segments>\n'
        for s in self.segments:
            nzb += s.nzb() + '\n'
        nzb += '</segments>\n</file>'
        return nzb

nzb_prefix = """<?xml version="1.0" encoding="iso-8859-1" ?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.0//EN" "http://www.nzbindex.com/nzb-1.0.dtd">
<!-- NZB Generated by Nuus -->
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"""
nzb_suffix = """</nzb>
"""

# UTILS
def _write_nzb_for_file(f):
    """http://wiki.sabnzbd.org/nzb-specs"""
    try:
        os.makedirs('nzbs')
    except OSError:
        pass
    osfn = os.path.join('nzbs',f.name + '.nzb')
    if os.path.exists(osfn):
        print osfn, 'already exists'
    else:
        osf = open(osfn, 'w')
        osf.write(nzb_prefix + '\n')
        osf.write(f.nzb() + '\n')
        osf.write(nzb_suffix)
        osf.close()

def gen_file_key(release_name, release_part, release_total, file_name, file_total_parts, poster):
    """hashes enough details about a file in hopes it will be unique"""
    m = md5.new()
    m.update(file_name)
    m.update(file_total_parts)
    m.update(release_name)
    m.update(release_part)
    m.update(release_total)
    m.update(poster)
    return m.hexdigest()

def dt2ut(dt):
    """datetime to unixtime"""
    return int(time.mktime(dt.timetuple()))

# (regex to match a date, function to parse the date to unix time)
date_formats = [
    (re.compile('^[0-9]+(?:\.[0-9]+)?$'), int),
    (re.compile('^[0-9]{2} [\w]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} .+$'), 
     lambda x: dt2ut(datetime.strptime(x, '%d %b %Y %H:%M:%S %Z'))),
    (re.compile('^[\w]{3}, [0-9]{2} [\w]{3} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4}'), 
     lambda x: dt2ut(datetime.strptime(x[:-6], '%a, %d %b %Y %H:%M:%S') - timedelta(hours=int(x[-5:])/100))),
]

def parse_date(datestr):
    """Checks common date formats in an attempt to convert a string into a date (returns unixtime)"""
    for p, fn in date_formats:
        if p.match(datestr):
            return fn(datestr)
    raise ValueError('Unable to handle date format: "%s"' % datestr)
        

class Parser(object):
    def __init__(self):
        self.releases = dict()
        self.files = dict()

    def process_article(self, group, post_number, subject, poster, 
                        date, article_id, references, size, lines):
        if subject is None:
            return
        for p in patterns:
            m = p.match(subject)
            if m:
                release_name, release_part, release_total, file_name, file_part, file_total = (
                    m.group('release_name'), m.group('release_part'), m.group('release_total'), 
                    m.group('file_name'), m.group('file_part'), m.group('file_total')
                )
                filekey = gen_file_key(release_name, release_part, release_total, file_name, file_total, poster)
                file = self.files.get(filekey, None)
                if file is None:
                    file = File(filekey, file_name, int(file_total), poster)
                    self.files[filekey] = file
                file.insert_part(group, article_id[1:-1], int(file_part), int(size), subject, parse_date(date))
                if file.complete():
                    _write_nzb_for_file(file)
                return
        # TODO: log unparsable file name
        

if __name__ == '__main__':
    parser = Parser()
    for group in os.listdir('cache'):
        for page in os.listdir(os.path.join('cache',group)):
            f = gzip.open(os.path.join('cache',group,page), 'r')
            articles = pickle.load(f)
            f.close()
            for article in articles:
                parser.process_article(group, *article)
                    