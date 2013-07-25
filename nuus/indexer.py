import cPickle as pickle
import datetime
import gzip
from multiprocessing import Process
import os
import redis
import sys
import time
import traceback
import re
from sqlalchemy.orm import scoped_session, sessionmaker

import nuus
from nuus import usenet, models
from nuus.utils import rkey

def build_articles(group, articles):
    arts = []
    grps = []
    for number, subject, poster, date, art_id, references, size, lines in articles:
        if art_id is None:
            continue
        # try process date
        ddate = None
        try:
            ddate = datetime.datetime.strptime(date, '%d %b %Y %H:%M:%S %Z')
        except:
            try:
                delta = int(date[-5:])
                delta = datetime.timedelta(hours=delta/100)
                ddate = datetime.datetime.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S')
                ddate -= delta
            except:
                print 'UNKNOWN DATE FORMAT:', date
        # set some defaults
        # create article
        art = dict(
            id=art_id[1:-1], 
            subject=subject,
            poster=poster, 
            size=int(size), 
            date=ddate
        )
        grp = dict(
            article=art_id[1:-1],
            group=group,
            number=int(number)
        )
        #session.merge(art)
        #session.add(grp) # this should never show up twice, but merging anyway
        arts.append(art)
        grps.append(grp)
    return (arts, grps)

class SQLAlchemyWriter(object):
    def __init__(self):
        self.db_session = scoped_session(
            sessionmaker(autocommit=False,
                         autoflush=False,
                         bind=models.engine()))

    def dump_articles(self, group, articles):
        arts, grps = build_articles(group, articles)
        con = db_session.connection()
        trans = con.begin()
        con.execute(models.Article.__table__.insert(), arts)
        con.execute(models.ArticleGroup.__table__.insert(), grps)
        trans.commit()
        con.close()
        #for a in arts:
        #    db_session.merge(a)
        #db_session.add_all(arts)
        #db_session.add_all(grps)

class FileSystemWriter(object):
    def __init__(self):
        pass
    def dump_articles(self, group, articles):
        try:
            os.makedirs(os.path.join('cache',group))
        except OSError:
            pass
        f = gzip.open(os.path.join('cache',group,articles[0][0]), 'w')
        pickle.dump(articles, f)
        f.close()

def UsenetWorker(wkr_id, writer_cls=FileSystemWriter, redis_connection_pool=nuus.redis_pool, usenet_connection_pool=nuus.usenet_pool):
    """
    statuses:
        new: just created
        idle: blocked on the input queue
        working: working on a task
        dead: killed
    """
    r = redis.StrictRedis(connection_pool=redis_connection_pool)
    u = usenet.Usenet(connection_pool=usenet_connection_pool)
    w = writer_cls()
    while True:
        r.set(rkey('w', wkr_id, 'status'), 'idle')
        # set start time
        start_time = time.time()
        # get task
        task = r.blpop(Indexer._taskqueue_key)
        task = pickle.loads(task[1])
        # run task
        r.set(rkey('w', wkr_id, 'group'), task.group)
        r.set(rkey('w', wkr_id, 'start'), task.start)
        r.set(rkey('w', wkr_id, 'end'), task.end)
        r.set(rkey('w', wkr_id, 'status'), 'working')
        print '/%s/' % wkr_id,
        articles = u.get_articles(task.group, task.start, task.end)
        print '|%s|' % wkr_id,
        w.dump_articles(task.group, articles)
        print '\\%s\\' % wkr_id,
        time_taken = time.time() - start_time
        # add the time taken to the completed tasks so the progress worker can
        # figure out the ETA
        r.rpush(Indexer._info_key,  pickle.dumps((wkr_id, time_taken)))
    return x

def ProgressWorker():
    """Processes progress information"""
    pass

class Task(object):
    def __init__(self, group, start, end):
        self.group = group
        self.start = start
        self.end = end

class Indexer(object):
    _taskqueue_key = rkey('i','q')
    _articles_key = rkey('i','a')
    _info_key = rkey('i','c')
    def __init__(self, articles_per_worker=10000, max_workers=8,
                 redis_connection_pool=nuus.redis_pool, 
                 usenet_connection_pool=nuus.usenet_pool):
        self._articles_per_worker = articles_per_worker
        self._max_workers = max_workers
        self._redis = redis.StrictRedis(connection_pool=redis_connection_pool)
        self._usenet = usenet.Usenet(connection_pool=usenet_connection_pool)
        self._workers = []

    def _create_tasks(self, group, start, end):
        # split up start - end into chunks of articles_per_worker length
        while start <= end:
            self._redis.rpush(
                self._taskqueue_key,
                # pickle new task object
                pickle.dumps(Task(group, start, min(end, start+self._articles_per_worker-1))))
            start += self._articles_per_worker

    def run(self):
        # launch workers
        self.start_workers()
        # check for new articles
        self.update_articles()
        # watch for completed tasks
        last_report = completed = 0
        total_time_taken = [0.0 for i in xrange(len(self._workers))]
        final_check = False
        while True:
            time.sleep(0.1)
            while self._redis.llen(self._info_key) > 0:
                comp = self._redis.lpop(self._info_key)
                if comp is None: # shouldn't happen, but i'm covering my bases
                    continue
                comp = pickle.loads(comp)
                wkr = comp[0]
                total_time_taken[wkr] += comp[1]
                completed += 1
            tasks_left = self._redis.llen(self._taskqueue_key)
            if tasks_left == 0:
                if all(self._redis.get(rkey('w', i, 'status')
                ) in ['idle', 'dead'] for i in xrange(len(self._workers))):
                    if not final_check:
                        # do one last check, just incase some of the workers had just
                        # grabbed a new task
                        final_check = True
                        continue
                    break
            elif completed > last_report:
                avg_total_time = reduce(lambda a,b: a+b, total_time_taken) / len(self._workers)
                avg_time_per_task = avg_total_time / completed
                eta = avg_time_per_task * tasks_left
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
                print 'Completed: {0}, Remaining: {1}, AVG time/task: {2:.3}s, ETR: {3}'.format(
                    completed, tasks_left, avg_time_per_task, eta)
                last_report = completed
        print 'all tasks completed'
        self._redis.save()
        self.shutdown()

    def shutdown(self):
        """shutdown worker processes"""
        for w in self._workers:
            w.terminate()

    def start_workers(self):
        print 'starting workers...',
        # relaunch any stalled workers
        for key in self._redis.keys(rkey('w','*','status')):
            wkr_id = rkey.split(key)[1]
            if self._redis.get(key) == 'working':
                # if the worker is marked as working, get the task it was working on
                p = self._redis.pipeline()
                p.get(rkey('w', wkr_id, 'group')).delete(rkey('w', wkr_id, 'group'))
                p.get(rkey('w', wkr_id, 'start')).delete(rkey('w', wkr_id, 'start'))
                p.get(rkey('w', wkr_id, 'end')).delete(rkey('w', wkr_id, 'end'))
                p.delete(rkey('w', wkr_id, 'progress'))
                p.set(rkey('w', wkr_id, 'status'), 'dead')
                res = p.execute()
                # redistribute the incomplete task back onto the task queue
                self._create_tasks(res[0], int(res[2]), int(res[4]))
        for i in xrange(self._max_workers):
            print i,
            worker = Process(target=UsenetWorker, args=(i,))
            worker.start()
            self._workers.append(worker)
        print ''
        
    def update_articles(self):
        print 'updating articles...'
        for key in self._redis.keys(rkey('group','*','last_article')):
            group = rkey.split(key)[1]
            print group,
            last_checked = int(self._redis.get(key))
            ginfo = self._usenet.group_info(group)
            last_checked = max(ginfo.first-1, last_checked)
            if ginfo.last > last_checked:
                print last_checked, '->', ginfo.last
                self._create_tasks(group, last_checked+1, ginfo.last)
                self._redis.set(rkey('group',group,'last_article'), ginfo.last)
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
        
