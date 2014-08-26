from concurrent.futures import ProcessPoolExecutor, as_completed
import gzip
import nuus
from nuus import usenet, usenet_pool
from nuus import ranges
from nuus.indexer import parser
from nuus.utils import time_remaining
import os
import pickle
import re
import sys
import time
from uuid import uuid1

BLOCK_STORAGE_DIR = nuus.app.config.get('BLOCK_STORAGE_DIR')
BLOCK_FILE_FORMAT = nuus.app.config.get('BLOCK_FILE_FORMAT')
BLOCK_FILE_REGEX = re.compile(nuus.app.config.get('BLOCK_FILE_REGEX'))

def download_headers(uuid, group, start, end):
    articles = []
    fn = None
    try:
        fn = BLOCK_FILE_FORMAT.format(
            group=group,start=start,end=end)
        u = usenet.Usenet(connection_pool=usenet_pool)
        articles = u.get_articles(group, start, end)
        if articles:
            with gzip.open(os.path.join(BLOCK_STORAGE_DIR, fn), 'w') as f:
                pickle.dump(articles, f)
        else:
            fn = None
    except Exception as e:
        print('GOT:', e)
    return (uuid, len(articles), fn)

def get_existing_blocks(group):
    r = []
    for fn in os.listdir(BLOCK_STORAGE_DIR):
        m = BLOCK_FILE_REGEX.match(fn)
        if m and m.group('group') == group:
            r.append((int(m.group('start')),
                      int(m.group('end'))))
    for fn in os.listdir(os.path.join(BLOCK_STORAGE_DIR, 'complete')):
        m = BLOCK_FILE_REGEX.match(fn)
        if m and m.group('group') == group:
            r.append((int(m.group('start')),
                      int(m.group('end'))))
    # TODO: clean up range util
    return ranges.merge(r)

def generate_tasks(group, start_at=0, end_at=None, articles_per_task=5000, limit=None):
    if (end_at is not None and start_at > end_at):
        print('got start > end for: %s from %s -> %s' % (group, start_at, end_at))
        return []
    u = usenet.Usenet(connection_pool=usenet_pool)
    tasks = []
    gi = u.group_info(group)
    if (gi.first == gi.last):
        print(group, 'has no articles!!!')
        raise Exception('no articles')
    st = max(gi.first, start_at)
    end_at = min(end_at, gi.last) if end_at else gi.last
    print('creating tasks for %s from %s -> %s' % (group, st, end_at))
    while st <= end_at and (limit is None or len(tasks) < limit):
        end = min(end_at, st+articles_per_task)
        tasks.append(dict(uuid=uuid1(),group=group,start=st,end=end))
        st = end+1
    return tasks

def run_tasks(tasks):
    print('Starting header downloads with %s tasks' % len(tasks))
    start_time = time.time()
    new_articles = 0
    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = []
        # queue up tasks
        for task in tasks:
            futures.append(executor.submit(download_headers, **task))
        print('all tasks queued...')
        # get results
        tasks_complete = 0
        report_status_on = int(len(tasks) / 100) or 1
        for f in as_completed(futures):
            try:
                tid, arts, fn = f.result()
            except Exception as exc:
                print('got an exception: %s' % exc)
            else:
                new_articles += arts
                tasks_complete += 1
                if tasks_complete % report_status_on == 0:
                    print('Completed: %s, Remaining: %s, ETR: %s' % (
                        tasks_complete, len(tasks) - tasks_complete, 
                        time_remaining(start_time, tasks_complete, len(tasks) - tasks_complete)))
    return (new_articles, time.time() - start_time)

def run(groups, articles_per_task=20000):
    tasks = []
    for group in groups:
        if group.startswith('a.b.'):
            group = 'alt.binaries.' + group[4:]
        start_at = 0
        blocks = get_existing_blocks(group)
        try:
            for i in range(1, len(blocks)):
                tasks += generate_tasks(group, start_at=blocks[i-1][1]+1, end_at=blocks[i][0]-1, articles_per_task=articles_per_task)
            if len(blocks):
                start_at = blocks[-1][1]+1
            tasks += generate_tasks(group, articles_per_task=articles_per_task, start_at=start_at)
        except:
            continue
    rval = run_tasks(tasks)
    print('... completed')
    return rval

if __name__ == '__main__':
    run(sys.argv[1:])
