import cPickle as pickle
from collections import deque
import datetime
import gzip
from multiprocessing import Process, Queue, JoinableQueue, queues
import os
import sys
import time

import nuus
from nuus import usenet
from nuus.database import Database
from nuus.utils import rkey
from nuus.cache import utils as cache_utils
from parser import parse_date

def check_cache(group, start):
    return os.path.exists(os.path.join('cache',group,str(start)))
    
def dump_articles(group, articles):
    try:
        os.makedirs(os.path.join('cache',group))
    except OSError:
        pass
    f = gzip.open(os.path.join('cache',group,articles[0][0]), 'w')
    pickle.dump(articles, f)
    f.close()

def UsenetWorker(wkr_id, input_queue, output_queue, 
                 usenet_connection_pool=nuus.usenet_pool):
    u = usenet.Usenet(connection_pool=usenet_connection_pool)
    while True:
        # set start time
        start_time = time.time()
        # get task
        task = input_queue.get()
        if task is None:
            continue
        #print wkr_id, 'processing', task.id
        if not check_cache(task.group, task.start):
            # run task
            articles = u.get_articles(task.group, task.start, task.end)
            if articles:
                dump_articles(task.group, articles)
        else:
            sys.stdout.write('-')
        # add the time finished for ETA calc
        input_queue.task_done()
        output_queue.put((task, time.time()))

class Task(object):
    def __init__(self, id, group, start, end, complete=False):
        self.id = id
        self.group = group
        self.start = start
        self.end = end
        self.complete = complete

class Indexer(object):
    def __init__(self, articles_per_worker=10000, max_workers=8,
                 usenet_connection_pool=nuus.usenet_pool):
        self._articles_per_worker = articles_per_worker
        self._max_workers = max_workers
        self._usenet = usenet.Usenet(connection_pool=usenet_connection_pool)
        self._workers = []
        self._taskqueue = JoinableQueue()
        self._resultqueue = Queue()
        self._db = Database('indexer.db')

    def _create_tasks(self, group, start, end):
        # split up start - end into chunks of articles_per_worker length
        while start <= end:
            tid = rkey('t','g', start)
            if self._db.get(tid) is None:
                task = Task(tid, group, start, 
                            min(end, start+self._articles_per_worker-1))
                self._db.set(tid, task)
            start += self._articles_per_worker

    def run(self):
        # launch workers
        self.start_workers()
        # check for new articles
        self.update_articles()
        # populate task queue
        tasks_left = 0
        for k in self._db.keys():
            pfx = rkey('t','g')
            if isinstance(k, basestring) and k.startswith(pfx):
                task = self._db.get(k)
                self._taskqueue.put(task)
                tasks_left += 1
        # watch for completed tasks
        completed = 0
        start_time = time.time()
        last_time = time.time()
        while True:
            try:
                task, time_taken = self._resultqueue.get(timeout=1)
            except queues.Empty:
                continue
            completed +=1
            tasks_left -= 1
            self._db.delete(task.id)
            if tasks_left <= 0:
                break
            elif completed % 10 == 0 or tasks_left < 50:
                avg_time = (time_taken - start_time) / completed
                eta = avg_time * tasks_left
                eta_secs = eta % 60
                eta -= eta_secs
                eta /= 60
                eta_mins = eta % 60
                eta -= eta_mins
                eta /= 60
                eta_hours = eta
                eta = ''
                if eta_hours:
                    eta = '%sh' % eta_hours
                if eta_mins:
                    eta += '%sm' % eta_mins
                elif eta_hours:
                    eta += '0m'
                eta += '%ss' % int(eta_secs)
                print 'Completed: %s, Remaining: %s, ETR: %s' % (
                    completed, tasks_left, eta)
        print 'all tasks completed'
        self.shutdown()

    def shutdown(self):
        """shutdown worker processes"""
        for w in self._workers:
            w.terminate()

    def start_workers(self):
        print 'starting workers...'
        for i in xrange(self._max_workers):
            print i,
            worker = Process(target=UsenetWorker, 
                             args=(i,self._taskqueue,self._resultqueue))
            worker.start()
            self._workers.append(worker)
        
    def update_articles(self):
        print 'updating articles...'
        gf = open('groups.txt', 'r')
        groups = gf.readlines()
        for group in (g.strip() for g in groups):
            if group.startswith('#') or group == '':
                continue
            print group,
            last_article_key = rkey('g',group,'la')
            last_checked = self._db.get(last_article_key) or 0
            ginfo = self._usenet.group_info(group)
            last_checked = max(ginfo.first-1, last_checked)
            if ginfo.last > last_checked:
                print last_checked, '->', ginfo.last
                self._create_tasks(group, last_checked+1, ginfo.last)
                self._db.set(last_article_key, ginfo.last)
            else:
                print 'up to date'
        print 'done'

if __name__ == '__main__':
    indexer = Indexer()
    try:
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        indexer.run()
    except KeyboardInterrupt:
        print 'Shutting down'
        indexer.shutdown()
        
