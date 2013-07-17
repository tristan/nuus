from itertools import chain
from functools import wraps
import os
import socket
import nntplib
import cPickle as pickle

class ConnectionError(Exception):
    pass

class UsenetConnection(object):
    def __init__(self, host, port=119,
                 username=None, password=None):
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.nntp = None

    def connect(self):
        if self.nntp:
            return
        try:
            self.nntp = nntplib.NNTP(self.host, self.port, self.username, self.password)
        except socket.error:
            e = sys.exc_info()[1]
            raise ConnectionError("Error connecting to %s:%s. %s" % 
                                  (self.host, self.port, e.args[0]))

    def disconnect(self):
        if self.nntp is None:
            return
        try:
            self.nntp.quit()
        except socket.error:
            pass
        self.nntp = None

    def _execute_fn(self, fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except socket.error:
            raise # TODO: try reconnect?        


class ConnectionPool(object):
    def __init__(self, connection_class=UsenetConnection, max_connections=None, **connection_kwargs):
        self.pid = os.getpid()
        self.max_connections = max_connections
        self.connection_kwargs = connection_kwargs
        self.connection_class = connection_class
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.__init__(self.connection_class, self.max_connections,
                          **self.connection_kwargs)

    def get_connection(self):
        self._checkpid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        if self.max_connections and self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    def release(self, connection):
        self._checkpid()
        if connection.pid == self.pid:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)

    def disconnect(self):
        all_conns = chain(self._available_connections, 
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()

class GroupInfo(object):
    def __init__(self, res):
        self.response = int(res[0].split(' ')[0])
        self.count = int(res[1])
        self.first = int(res[2])
        self.last = int(res[3])
        self.name = res[4]

# TODO: this is wrong if i want to actually set max_retries
def retry(fn, max_retries=16):
    """Wraps function in a try except to capture EOFErrors and retries the
    function if the error is captured. Backs off with each consecutive 
    failure.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        retries=0
        while retries < max_retries:
            try:
                return fn(*args, **kwargs)
            except EOFError as e:
                print 'got EOFError, retrying...'
                retries+=1
                time.sleep(retries)
        print 'max number of retries reached.'
        raise e

# TODO: handle random EOFError
class Usenet(object):
    def __init__(self, host=None, port=119,
                 username=None, password=None,
                 connection_pool=None, use_cache=False):
        self.connection_pool = connection_pool
        self.use_cache = use_cache
        if not connection_pool:
            if not host:
                raise ValueError('either host or connection_pool must not be null')
            self.connection_pool = ConnectionPool(
                host=host, port=port, username=username, password=password)
    
    @retry
    def group_info(self, group):
        conn = self.connection_pool.get_connection()
        conn.connect()
        res = conn.nntp.group(group)
        conn.disconnect()
        self.connection_pool.release(conn)
        return GroupInfo(res)

    @retry
    def get_articles(self, group, start, end):
        articles = []
        if start <= end:
            # we didn't get everything from cache
            conn = self.connection_pool.get_connection()
            conn.connect()
            resp, count, first, last, name = conn.nntp.group(group)
            start = min(int(last), max(start, int(first)))
            end = max(int(first), min(end, int(last)))
            resp, items = conn.nntp.xover(str(start), str(end))
            # check that items are sequential and add empty articles when they're not
            idx = 0
            while start + idx <= end:
                #print start, end, idx, len(items), items[idx][0]
                if idx >= len(items) or int(items[idx][0]) != (start + idx):
                    items.insert(idx, (str(start + idx), None, None, None, None, None, None))
                idx += 1
            articles.extend(items)
            conn.disconnect()
            self.connection_pool.release(conn)
        return articles
