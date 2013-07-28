from collections import namedtuple, deque
import cPickle as pickle
import md5
import os
import pickle
import struct
import threading
import zlib

def pack_int(int):
    return struct.pack("!i", int)

def unpack_int(bytes):
    return struct.unpack("!i", bytes)[0]

def pack_uint(uint):
    assert uint >= 0, "%r < 0" %(uint, )
    return struct.pack("!I", uint)

def unpack_uint(bytes):
    return struct.unpack("!I", bytes)[0]

def md5hash(string):
    m = md5.new()
    m.update(string)
    return m.digest()

class DatabaseError(Exception):
    pass

DBRecord = namedtuple("DBRecord", "db start end keyhash event data error")
class IndexItem(object):
    def __init__(self, key, start):
        self.key = key
        self.start = start

class SpecialSerializer(object):
    @classmethod
    def dumps(cls, string):
        return zlib.compress(pickle.dumps(string), 9)
    @classmethod
    def loads(cls, string):
        return pickle.loads(zlib.decompress(string))

class SimpleDatabase(object):
    """ Only has 2 types of events, set (\x00) and delete (\x01).
    Keys must be unsigned integers.
    No checks on keys on delete
    """
    def __init__(self, filename):
        self._filename = filename
        exists = os.path.exists(self._filename)
        mode = exists and 'a+b' or 'w+b'
        self._file = open(self._filename, mode)
        self._write_pos = self._file.tell()
        self._dirty = True
        self._read_pos = 0
        self._cached_index = {}

    def close(self):
        self._file.close()

    def compact(self, filename=None):
        if filename is None:
            filename = self._filename + '.compacted'
        ndb = SimpleDatabase(filename)
        idx = self._index()
        for k in sorted(idx.keys()):
            r = self._read_one(idx[k])
            if r is None:
                continue
            t,k,o,s,e = r # o is pickled still
            ndb._write_one(k, t, o)
        ndb._file.close()

    def delete(self, key):
        self._write_one(key, '\x01', '')

    def _drop_cache(self):
        """Let the cache get garbage collected"""
        self._read_pos = 0
        self._dirty = True
        self._cached_index = {}

    def _index(self):
        if not self._dirty:
            return self._cached_index
        while True:
            r = self._read_one(self._read_pos)
            if r is None:
                break
            t,k,o,s,e = r
            if t == '\x01':
                del self._cached_index[k]
            else:
                self._cached_index[k] = s
            self._read_pos = e
        self._dirty = False
        return self._cached_index

    def _read_one(self, start):
        HEADER_LEN=4+1+4
        CHECKSUM_LEN = 4
        self._file.seek(start)
        header = self._file.read(HEADER_LEN)
        header_len = len(header)
        if header_len == 0:
            return None

        if header_len < HEADER_LEN:
            raise DatabaseError("database is corrupt: unexpected header length")
        key = unpack_uint(header[:4])
        event = header[4]
        data_checksum_len = unpack_uint(header[5:]) + CHECKSUM_LEN
        end = start + HEADER_LEN + data_checksum_len
        data_checksum = self._file.read(data_checksum_len)
        if len(data_checksum) != data_checksum_len:
            raise DatabaseError("database is corrupt: unexpected data length")
        data = data_checksum[:-CHECKSUM_LEN]
        checksum = data_checksum[-CHECKSUM_LEN:]
        actual_checksum = pack_int(zlib.crc32(header + data))
        if actual_checksum != checksum:
            raise DatabaseError("failed checksum")

        return (event, key, data, start, end)

    def set(self, key, obj):
        self._write_one(key, '\x00', pickle.dumps(obj))

    def _write_one(self, key, event, data):
        self._file.seek(self._write_pos)
        data_len = pack_uint(len(data))
        packed_key = pack_uint(key)
        to_write = packed_key + event + data_len + data
        to_write += pack_int(zlib.crc32(to_write))
        self._file.write(to_write)
        self._file.flush()
        self._write_pos += len(to_write)
        self._dirty = True

class Database(object):
    """    """
    SERIALIZER = pickle

    EVENT_CREATE = '\x00'
    EVENT_DELETE = '\x01'
    EVENT_SET = '\x02'

    VALID_EVENTS = [chr(x) for x in xrange(3)]
    

    def __init__(self, filename, create=False):
        self.filename = filename
        self._file = self._open(create)
        self._initialize()

    def __len__(self):
        return self._item_count

    def close(self):
        self._file.close()

    def compact(self):
        cdb = Database(self.filename + '.compacted')
        for k in self.keys():
            o = self.get(k)
            cdb.set(k, o)
        cdb.close()

    def _initialize(self):
        self._file.seek(0)
        self._write_pos = self._file.tell()
        self._item_count = 0
        self._index = dict()
        while True:
            record = self._read_one(self._write_pos)
            if record is None:
                break
            if record.event == self.EVENT_CREATE:
                if record.keyhash in self._index:
                    raise DatabaseError("got create event twice for key: %s <%s>"
                                        % (self.SERIALIZER.loads(record.data), record.keyhash))
                self._index[record.keyhash] = IndexItem(self.SERIALIZER.loads(record.data), -1)
                self._item_count += 1
            elif record.event == self.EVENT_DELETE:
                if not (record.keyhash in self._index):
                    raise DatabaseError("attempting to delete non-existant keyhash: %s" % record.keyhash)
                del self._index[record.keyhash]
                self._item_count -= 1
            elif record.event == self.EVENT_SET:
                if not (record.keyhash in self._index):
                    raise DatabaseError("attempting to set non-existant keyhash: %s" % record.keyhash)
                self._index[record.keyhash].start = record.start
            self._write_pos = record.end

    def _hash_key(self, key):
        serialized_key = self.SERIALIZER.dumps(key)
        return md5hash(serialized_key), serialized_key

    def _open(self, create):
        exists = os.path.exists(self.filename)
        #if create and exists:
        #    raise DatabaseError("refusing to create - file already exists: %r" % self.filename)
        #elif not (create or exists):
        #    raise DatabaseError("file does not exist: %r" % self.filename)
        mode = (create or not exists) and 'w+b' or 'r+b'
        return open(self.filename, mode)

    def _read_one(self, start):
        HEADER_LEN=16+1+4
        CHECKSUM_LEN = 4
        self._file.seek(start)
        header = self._file.read(HEADER_LEN)
        header_len = len(header)
        if header_len == 0:
            return None

        if header_len < HEADER_LEN:
            raise DatabaseError("database is corrupt: unexpected header length")
        keyhash = header[:16]
        event = header[16]
        if not (event in self.VALID_EVENTS):
            raise DatabaseError("got invalid event: %r" % event)
        data_checksum_len = unpack_uint(header[17:]) + CHECKSUM_LEN
        end = start + HEADER_LEN + data_checksum_len
        data_checksum = self._file.read(data_checksum_len)
        if len(data_checksum) != data_checksum_len:
            raise DatabaseError("database is corrupt: unexpected data length")
        data = data_checksum[:-CHECKSUM_LEN]
        checksum = data_checksum[-CHECKSUM_LEN:]
        actual_checksum = pack_int(zlib.crc32(header + data))
        if actual_checksum != checksum:
            raise DatabaseError("failed checksum")

        return DBRecord(self, start, end, keyhash, event, data, None)

    def _write_one(self, keyhash, event, data):
        assert event in self.VALID_EVENTS, "invalid event type"
        self._file.seek(self._write_pos)
        data_len = pack_uint(len(data))
        to_write = keyhash + event + data_len + data
        to_write += pack_int(zlib.crc32(to_write))
        self._file.write(to_write)
        self._file.flush()
        self._write_pos += len(to_write)

    def delete(self, key):
        keyhash, _ = self._hash_key(key)
        self._delete(keyhash)

    def _delete(self, keyhash):
        if not (keyhash in self._index):
            return
        self._write_one(keyhash, self.EVENT_DELETE, '')
        del self._index[keyhash]
        self._item_count -= 1

    def get(self, key, default=None):
        keyhash, _ = self._hash_key(key)
        return self._get(keyhash) or default

    def _get(self, keyhash):
        if keyhash in self._index:
            record = self._read_one(self._index[keyhash].start)
            return self.SERIALIZER.loads(record.data)
        return None

    def keys(self, filter=None):
        return [x.key for x in self._index.values()]

    def set(self, key, obj):
        keyhash, serialized_key = self._hash_key(key)
        self._set(keyhash, serialized_key, obj)

    def _set(self, keyhash, serialized_key, obj):
        if not (keyhash in self._index):
            self._write_one(keyhash, self.EVENT_CREATE, serialized_key)
            self._index[keyhash] = IndexItem(self.SERIALIZER.loads(serialized_key), -1)
            self._item_count += 1
        self._index[keyhash].start = self._write_pos
        self._write_one(keyhash, self.EVENT_SET, self.SERIALIZER.dumps(obj))

class CachedDatabase(Database):
    def __init__(self, filename, cached_entries=100, auto_flush=True):
        """if auto_flush is true, all set and delete operations will be triggered
        when they happen, otherwise, set and delete will only hit the database when
        the object falls off the cache (or when .flush or .delete is called)"""
        super(CachedDatabase, self).__init__(filename)
        self._max_cached = cached_entries
        self._auto_flush = auto_flush
        self._cache = []
        self._cachekeys = []

    def _add(self, key, obj):
        if len(self._cachekeys) >= self._max_cached:
            if not self._auto_flush:
                self._flush_one()
            else:
                self._cache = self._cache[1:]
                self._cachekeys = self._cachekeys[1:]
        self._cache.append(obj)
        self._cachekeys.append(key)

    def close(self):
        self.flush()
        self._file.close()

    def delete(self, key):
        keyhash,_ = self._hash_key(key)
        self._delete(keyhash)
        i = self._find(key)
        if i >= 0:
            self._cache = self._cache[:i] + self._cache[i+1:]
            self._cachekeys = self._cachekeys[:i] + self._cachekeys[i+1:]

    def _find(self, key):
        i = len(self._cachekeys)-1
        while i >= 0:
            if self._cachekeys[i] == key:
                break
            i -= 1
        return i

    def flush(self):
        while len(self._cache):
            self._flush_one()

    def _flush_one(self):
        if len(self._cachekeys):
            key = self._cachekeys[0]
            obj = self._cache[0]
            keyhash,serialized_key = self._hash_key(key)
            self._set(keyhash, serialized_key, obj)
            self._cache = self._cache[1:]
            self._cachekeys = self._cachekeys[1:]

    def get(self, key):
        i = self._find(key)
        if i >= 0:
            return self._cache[i]
        keyhash,_ = self._hash_key(key)
        r = self._get(keyhash)
        if r:
            self._add(key, r)
        return r

    def set(self, key, obj):
        i = self._find(key)
        if i >= 0:
            if self._auto_flush:
                keyhash,_ = self._hash_key(key)
                self._index[keyhash].start = self._write_pos
                self._write_one(keyhash, self.EVENT_SET, self.SERIALIZER.dumps(obj))
            self._cache[i] = obj
        else:
            if self._auto_flush:
                keyhash,serialized_key = self._hash_key(key)
                self._set(keyhash, serialized_key, obj)
            self._add(key, obj)
