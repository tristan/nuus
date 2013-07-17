from nuus.utils import rkey

class BasicWrapper(object):
    """Wraps a redis key pattern to give the appearance of an object"""
    def __init__(self, redis, kind, id, sets=[], lists=[]):
        self._redis = redis
        self._sets = sets
        self._lists = lists
        self._id = id
        self._key_root = rkey(kind, id)

    def __getitem__(self, key, default=None):
        if key == 'id':
            return self._id
        if key in self._sets:
            return self._redis.smembers(rkey(self._key_root, key))
        elif key in self._lists:
            return self._redis.lrange(rkey(self._key_root, key), 0, -1)
        return self._redis.get(rkey(self._key_root, key)) or default

def FileWrapper(redis, subj):
    return BasicWrapper(redis, 'file', subj, sets=['groups', 'segments'])

class ReleaseWrapper(BasicWrapper):
    def __init__(self, redis, subj):
        super(ReleaseWrapper, self).__init__(redis, 'release', subj, sets=['files'])

    def __getitem__(self, key, default=None):
        rval = super(ReleaseWrapper, self).__getitem__(key, default)
        if key == 'files':
            rval = [FileWrapper(self._redis, k) for k in rval]
        return rval

def GroupWrapper(redis, group):
    return BasicWrapper(redis, 'group', group)
