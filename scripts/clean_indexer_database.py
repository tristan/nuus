import cPickle as pickle
from itertools import count, groupby
import gzip
from nuus.indexer import Task
from nuus.utils import rkey
from nuus.database import Database, CachedDatabase, SimpleDatabase
from nuus.cache.utils import old_cache_to_new_cache, _open_cache_db, cached_ranges
from nuus import usenet_pool
from nuus.usenet import Usenet
import os
import sys

gs = {}

if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    for g in os.listdir('cache'):
        gf = os.path.join('cache', g)
        if os.path.isdir(gf):
            db,created = _open_cache_db(g)
            if created:
                old_cache_to_new_cache(g, db=db)
            else:
                print 'skipping', g

    print '\ncleaning up tasks...'
    print 'loading db...'
    _db = Database('indexer.db')
    groups = []
    for k in _db.keys():
        pfx = rkey('t','g')
        if isinstance(k, basestring) and k.startswith(pfx):
            _db.delete(k)
        elif isinstance(k, basestring) and k.startswith('g:') and k.endswith(':la'):
            _db.set(k, 0)
            groups.append(rkey.split(k)[1])

    # create tasks for missing 
    usenet = Usenet(connection_pool=usenet_pool)
    _articles_per_worker = 10000
    def _create_tasks(self, group, start, end):
        # split up start - end into chunks of articles_per_worker length
        while start <= end:
            tid = rkey('t','g', start)
            if _db.get(tid) is None:
                task = Task(tid, group, start, 
                            min(end, start+_articles_per_worker-1))
                _db.set(tid, task)
            start += _articles_per_worker
    for group in groups:
        db,created = _open_cache_db(g)
        ranges = cached_ranges(db=db)
        ginfo = usenet.group_info(group)
        start = ginfo.first
        while len(ranges) > 0:
            if start < ranges[0][0]:
                _create_tasks(group, start, ranges[0][0]-1)
            start = ranges[0][1]+1
            ranges = ranges[1:]
        # add remaining
        if start < ginfo.last:
            _create_tasks(group, start, ginfo.last)
        # make sure last article is set correctly
        _db.set(rkey('g',group,'la'), ginfo.last)
        
    print '\ncompacting db...'
    _db.compact()
