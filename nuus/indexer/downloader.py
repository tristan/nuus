from concurrent.futures import ProcessPoolExecutor, as_completed
import gzip
import nuus
from nuus import usenet, usenet_pool
from nuus.utils import time_remaining
from nuus import ranges
import os
import pickle
import re
import sys
import time

BLOCK_STORAGE_DIR = nuus.app.config.get('BLOCK_STORAGE_DIR')
BLOCK_FILE_FORMAT = nuus.app.config.get('BLOCK_FILE_FORMAT')
BLOCK_FILE_REGEX = re.compile(nuus.app.config.get('BLOCK_FILE_REGEX'))

def download_headers(id, group, start, end):
    fn = os.path.join(BLOCK_STORAGE_DIR, BLOCK_FILE_FORMAT.format(
        group=group,start=start,end=end))
    u = usenet.Usenet(connection_pool=usenet_pool)
    articles = u.get_articles(group, start, end)
    if articles:
        with gzip.open(fn, 'w') as f:
            pickle.dump(articles, f)
    return (id, len(articles))

def get_existing_blocks(group):
    r = []
    for fn in os.listdir(BLOCK_STORAGE_DIR):
        m = BLOCK_FILE_REGEX.match(fn)
        if m:
            r.append((int(m.group('start')),
                      int(m.group('end'))))
    # TODO: clean up range util
    return ranges.merge(r)

def generate_tasks(group, id=0, start_at=0, articles_per_task=5000, limit=None):
    u = usenet.Usenet(connection_pool=usenet_pool)
    tasks = []
    gi = u.group_info(group)
    st = max(gi.first, start_at)
    print('creating tasks for %s from %s -> %s' % (group, st, gi.last))
    while st <= gi.last and (limit is None or len(tasks) < limit):
        end = min(gi.last, st+articles_per_task)
        tasks.append(dict(id=id,group=group,start=st,end=end))
        st = end+1
        id += 1
    return tasks

def run_tasks(tasks):
    print('Starting header downloads with %s tasks' % len(tasks))
    start_time = time.time()
    with ProcessPoolExecutor() as executor:
        futures = []
        # queue up tasks
        for task in tasks:
            futures.append(executor.submit(download_headers, **task))
        print('all tasks queued...')
        # get results
        new_articles = 0
        tasks_complete = 0
        report_status_on = int(len(tasks) / 100) or 1
        for f in as_completed(futures):
            try:
                tid, arts = f.result()
            except Exception as exc:
                print('got an exception: %s' % exc)
            else:
                new_articles += arts
                tasks_complete += 1
                if tasks_complete % report_status_on == 0:
                    print('Completed: %s, Remaining: %s, ETR: %s' % (
                        tasks_complete, len(tasks) - tasks_complete, 
                        time_remaining(start_time, tasks_complete, len(tasks) - tasks_complete)))

if __name__ == '__main__':
    group = sys.argv[1]
    articles_per_task = sys.argv[2] if len(sys.argv) > 2 else 100000
    blocks = get_existing_blocks(group)
    start_at = (blocks[-1][-1]+1) if len(blocks) else 0
    if group.startswith('a.b.'):
        group = 'alt.binaries.' + group[4:]
    tasks = generate_tasks(group, articles_per_task=articles_per_task, start_at=start_at)
    run_tasks(tasks)
    print('... completed %s' % group)
