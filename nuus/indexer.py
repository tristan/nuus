import os, sys
import time
import re
import cPickle as pickle
import nuus
import redis
from multiprocessing import Process
from nuus import usenet
from nuus.utils import rkey
from nntplib import NNTPError

def handle_no_match(group, item):
    fn = '%s_no_match.data' % group
    items = []
    if os.path.exists(fn):
        f = open(fn, 'r')
        items = pickle.load(f)
        f.close()
    f = open(fn, 'w')
    items.append(item)
    pickle.dump(items, f)
    f.close()

re_main1 = re.compile(r'^(.*)yEnc \(([0-9]+)\/([0-9]+)\)$')
re_main2 = re.compile(r'^(.*) \(([0-9]+)\/([0-9]+)\) yEnc.*$')
re_main3 = re.compile(r'^(.*) \(([0-9]+)\/([0-9]+)\)$')

re_nzb1 = re.compile(r'\[([0-9]+)\/([0-9]+)\]')
re_nzb2 = re.compile(r'\(([0-9]+)\/([0-9]+)\)')

re_file1 = re.compile(r'"([^\s]+\.part[0-9]+\.rar)"') # blah.part000.rar
re_file2 = re.compile(r'"([^\s]+\.r(?:[0-9]{2}|ar))"') # blah.rar blah.r01
re_file3 = re.compile(r'"([^\s]+\.n(?:fo|zb))"') # blah.nfo blah.nzb

re_scene1 = re.compile(r'^(.+)\s+-\s+\[([0-9]+)\/([0-9]+)\]\s+-\s+"([^"]+)".*$')

def key(*args):
    return ':'.join(args)

def process_items(group, items, handle_no_match=handle_no_match):
    files = {} # cache so we know not to recreate files
    pipe = redis.pipeline()
    for number, subject, poster, date, id, references, size, lines in items:
        #try:
        #    subject = u''.join(map(unichr, map(ord, subject)))
        #    subject = subject.encode('utf-8')
        #except UnicodeDecodeError:
        #    print subject, type(subject)
        # get segment id
        seg_id = id[1:-1]
        # create segment
        pipe.set(key('segment', seg_id, 'bytes'), size)
        m = re_main1.match(subject) or re_main2.match(subject) or re_main3.match(subject)
        if m:
            subj, part, total = m.groups()
            m = re_file1.search(subj) or re_file2.search(subj) or re_file3.search(subj)
            filename = m.group(1) if m else None
            #m = re_nzb1.search(subj) or re_nzb2.search(subj)
            #fseq, ftotal = m.groups() if m else (None, None)
            # set segment number
            pipe.set(key('segment', seg_id, 'number'), part)
            #file = models.File.get_or_create(subj, poster=poster, date=date, filename=filename, total_segments=total)
            groups = files.get(subj, None)
            if not groups:
                # see if the file already exists in redis
                groups = redis.smembers(key('file', subj, 'groups'))
                if groups:
                    files[subj] = groups
            if not groups:
                pipe.set(key('file', subj, 'poster'), poster)
                pipe.set(key('file', subj, 'date'), date)
                pipe.set(key('file', subj, 'filename'), filename)
                pipe.set(key('file', subj, 'total_segments'), total)
                pipe.sadd(key('file', subj, 'groups'), group)
            elif group not in groups:
                pipe.sadd(key('file', subj, 'groups'), group)
            pipe.sadd(key('file', subj, 'segments'), seg_id)
            m = re_scene1.match(subj)
            if m:
                title, part, total, filename = m.groups()
                pipe.sadd(key('release', title, 'files'), subj)
                pipe.set(key('release', title, 'total_files'), total)
            elif 'Trance.2013' in subj:
                print 'failed: """%s""" ' % subj,
        else:
            handle_no_match(group, (number, subject, poster, date, id, references, size, lines))
    sys.stdout.write('.')
    pipe.execute()


def fetch_articles(group, cache=True):
    global usenet

    try:
        os.makedirs(group)
    except:
        pass

    redis.set('group:%s:status' % group, 'updating')
    redis.set('group:%s:progress' % group, 0)
    resp, count, first, last, name = usenet.group(group)
    first, last = int(first), int(last)
    chunk_size = 10000
    current = redis.get('group:%s:last_article' % group)
    current = int(current) + 1 if current is not None else first
    first = current

    ef = open('error.log', 'w+')

    while current < last:
        #print 'getting chunk from %s starting at %s (%s%% done).' % (group, current, ((current - first) / float(last - first)) * 100 )
        if current + chunk_size >= last:
            chunk_size = last - current

        retries = 0
        while True:
            try:
                resp, items = usenet.xover(str(current), str(current + chunk_size))
                break
            except Exception as e:
                ef.write('ERROR %s %s' % (e.message, e.__dict__))
                if retries:
                    print 'waiting %s seconds before retrying' % retries
                    time.sleep(retries)
                retries += 1
                #usenet.reconnect()
                usenet = get_usenet_client()
                usenet.group(group)
                if e.message.startswith('412'):
                    items = []
                    break
        if items:
            if cache:
                f = os.path.join(group, items[0][0])
                f = open(f, 'w')
                pickle.dump(items, f)
                f.close()
            process_items(group, items)
        current += chunk_size
        progress = ((current - first) / float(last - first)) * 100
        #print progress, '%'
        redis.set('group:%s:last_article' % group, current)
        redis.set('group:%s:progress' % group, "{0:.2f}".format(progress))
    redis.set('group:%s:status' % group, 'done')
    redis.set('group:%s:updated' % group, time.time())
    ef.close()


def update_groups():
    redis.set('indexer:start', time.time())
    groups = redis.smembers('groups')
    for g in groups:
        redis.set('group:%s:status' % g, 'pending')
    for g in groups:
        fetch_articles(g)

def UsenetWorker(id, redis_connection_pool=nuus.redis_pool, usenet_connection_pool=nuus.usenet_pool):
    """
    statuses:
        new: just created
        idle: blocked on the input queue
        working: working on a task
        dead: killed
    """
    r = redis.StrictRedis(connection_pool=redis_connection_pool)
    u = usenet.Usenet(connection_pool=usenet_connection_pool)
    while True:
        r.set(rkey('worker', id, 'status'), 'idle')
        # set start time
        start_time = time.time()
        # get task
        task = pickle.loads(r.blpop(Indexer._taskqueue_key))
        # run task
        r.set(rkey('worker', id, 'group'), task.group)
        r.set(rkey('worker', id, 'start'), task.start)
        r.set(rkey('worker', id, 'end'), task.end)
        r.set(rkey('worker', id, 'status'), 'working')
        p = r.pipeline()
        for article in u.get_articles(task.group, task.start, task.end):
            number, subject, poster, date, id, references, size, lines = article
            p.set(rkey('article', id[1:-1], 'subject'), subject)
            p.set(rkey('article', id[1:-1], 'poster'), poster)
            p.set(rkey('article', id[1:-1], 'size'), size)
            p.set(rkey('article', id[1:-1], 'date'), date)
            p.sadd(rkey('article', id[1:-1], 'groups'), task.group)
        p.execute()
        time_taken = time.time() - start_time
        # add the time taken to the completed tasks so the progress worker can
        # figure out the ETA
        r.rpush('indexer:completedtasks', time_taken)
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
    _taskqueue_key = rkey('indexer','taskqueue')
    def __init__(self, articles_per_worker=10000, max_workers=4
                 redis_connection_pool=nuus.redis_pool, 
                 usenet_connection_pool=nuus.usenet_pool):
        self._articles_per_worker = articles_per_worker
        self._max_workers = max_workers
        self._redis = redis.StrictRedis(connection_pool=redis_connection_pool)
        self._usenet = usenet.Usenet(conncetion_pool=usenet_connection_pool)

    def _create_tasks(self, group, start, end):
        # split up start - end into chunks of articles_per_worker length
        while start <= end:
            self._redis.rpush(
                self._taskqueue_key,
                # pickle new task object
                pickle.dumps(Task(group, start, min(end, start+self.articles_per_worker))))
            start += self.articles_per_worker

    def run(self):
        # launch workers
        self.start_workers()
        # check for new articles
        self.update_articles()
        pass

    def shutdown(self):
        """shutdown worker processes"""
        pass

    def start_workers(self):
        # relaunch any stalled workers
        for key in self._redis.keys(rkey('worker','*','status')):
            wkr_id = rkey.split(key)[1]
            if self._redis.get(key) == 'working':
                # if the worker is marked as working, get the task it was working on
                p = self._redis.pipeline()
                p.get(rkey('worker', wkr_id, 'group')).delete(rkey('worker', wkr_id, 'group'))
                p.get(rkey('worker', wkr_id, 'start')).delete(rkey('worker', wkr_id, 'start'))
                p.get(rkey('worker', wkr_id, 'end')).delete(rkey('worker', wkr_id, 'end'))
                p.delete(rkey('worker', wkr_id, 'progress'))
                p.set(rkey('worker', wkr_id, 'status'), 'dead')
                res = p.execute()
                # redistribute the incomplete task back onto the task queue
                self._create_task(res[0], res[2], res[4])
        for i in xrange(self._max_workers):
            Worker(i)
        
    def update_articles(self):
        for key in self._redis.keys(rkey('group','*','last_article')):
            group = rkey.split(key)[1]
            last_checked = int(self._redis.get(key))
            ginfo = self._usenet.group_info(group)
            last_checked = max(ginfo.first, last_checked)
            if ginfo.last > last_checked:
                self._create_tasks(group, last_checked, ginfo.last)


if __name__ == '__main__':
    indexer = Indexer()
    try:
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        indexer.run()
    except KeyboardInturrupt:
        print 'Shutting down'
        
