"""
Cache storage:

blocks of 100000 articles
 - post goes into block: post number / 1000000
 - sort each block

Indexing:
 - organise tasks per block such that each worker can work on the block

"""

import cPickle as pickle
from collections import deque
import datetime
import gzip
import os
import sys
import time

from concurrent.futures import ProcessPoolExecutor, as_completed
import nuus
from nuus import usenet
from nuus.utils import rkey, swallow, parse_date, time_taken_to_str, time_remaining
from nuus.database import engine, tables
import re
from sqlalchemy.sql import select

CACHE_BASE = nuus.app.config.get('CACHE_BASE')
CACHE_INBOX = nuus.app.config.get('CACHE_INBOX')
CACHE_COMPLETE = nuus.app.config.get('CACHE_COMPLETE')

CACHE_FILE_FORMAT = nuus.app.config.get('CACHE_FILE_FORMAT')
CACHE_FILE_REGEX = re.compile(nuus.app.config.get('CACHE_FILE_REGEX'))
CACHE_LINE_FORMAT = nuus.app.config.get('CACHE_LINE_FORMAT')
CACHE_LINE_REGEX = re.compile(nuus.app.config.get('CACHE_LINE_REGEX'))

def enc(s):
    return s.decode('latin-1').encode('utf-8')

def download_headers(id, group, start, end):
    print '+ <%s>' % id
    fn = os.path.join(CACHE_INBOX, CACHE_FILE_FORMAT.format(
        group=group,page='%s-%s' % (id, start),status='new'))
    if os.path.exists(fn):
        print '< <%s>' % id
        return (id, 0)
    u = usenet.Usenet(connection_pool=nuus.usenet_pool)
    print '> <%s>' % id
    articles = u.get_articles(group, start, end)
    if articles:
        with gzip.open(fn, 'w') as f:
            for number, subject, poster, sdate, art_id, _, size, _ in articles:
                f.write(CACHE_LINE_FORMAT.format(
                    article_id=enc(art_id[1:-1]),
                    subject=enc(subject),
                    poster=enc(poster),
                    date=parse_date(sdate),
                    size=size))
    print '< <%s>' % id
    return (id, len(articles))

def parse_header(file_name):
    pass

class Indexer(object):
    def __init__(self, articles_per_worker=10000, max_workers=8,
                 usenet_connection_pool=nuus.usenet_pool):
        self._articles_per_worker = articles_per_worker
        self._max_workers = max_workers
        self._usenet = usenet.Usenet(connection_pool=usenet_connection_pool)

    def run(self):
        conn = engine.connect()
        # get groups
        groups = conn.execute(select([tables.groups])).fetchall()
        # create new tasks
        with conn.begin() as trans:
            for g in groups:
                # get group info
                print 'updating group:', g['group'],
                gi = self._usenet.group_info(g['group'])
                if g['last_post_checked']+1 < gi.last:
                    print g['last_post_checked'], '->', gi.last
                else:
                    print ''
                st = max(gi.first, g['last_post_checked']+1)
                while st <= gi.last:
                    end = min(gi.last, st+self._articles_per_worker)
                    conn.execute(tables.tasks.insert(), **dict(group=g['id'],start=st,end=end))
                    st = end+1
                conn.execute(tables.groups.update().values({
                    tables.groups.c.last_post_checked: gi.last
                }).where(tables.groups.c.id == g['id']))
        # reindex groups by id
        groups = {g['id']:g for g in groups}
        # get all tasks
        tasks = []
        for row in conn.execute(select([tables.tasks]).order_by(tables.tasks.c.id)):
            tasks.append(dict(id=row['id'],
                              group=groups[row['group']]['group'],
                              start=row['start'],
                              end=row['end']))

        # start workers
        futures = []
        print 'Starting header downloads with %s tasks' % len(tasks)
        start_time = time.time()
        with ProcessPoolExecutor(max_workers=self._max_workers) as executor:
            # queue up tasks
            for task in tasks:
                futures.append(executor.submit(download_headers, **task))
            print 'all tasks queued...'
            # get results
            new_articles = 0
            tasks_complete = 0
            report_status_on = (len(tasks) / 100) or 1
            for f in as_completed(futures):
                r = f.result()
                tid, arts = r
                r = conn.execute(tables.tasks.delete().where(tables.tasks.c.id == tid))
                new_articles += arts
                tasks_complete += 1
                if tasks_complete % report_status_on == 0:
                    print '\nCompleted: %s, Remaining: %s, ETR: %s' % (
                        tasks_complete, len(tasks) - tasks_complete, 
                        time_remaining(start_time, tasks_complete, len(tasks) - tasks_complete))
                print '- <%s>' % tid

if __name__ == '__main__':
    indexer = Indexer()
    try:
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        indexer.run()
    except KeyboardInterrupt:
        print 'forcing shutdown...'
