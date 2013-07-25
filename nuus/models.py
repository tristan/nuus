import nuus
from nuus.utils import rkey

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship, backref

def engine():
    #return create_engine('sqlite:///nuus.db', 
    #                     convert_unicode=True)
    return create_engine('postgresql+pg8000://tristan@localhost/nuus',
                         isolation_level='READ UNCOMMITTED')

Base = declarative_base()

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


class Article(Base):
    __tablename__ = 'articles'

    id = Column(String, primary_key=True)
    subject = Column(String)
    poster = Column(String)
    size = Column(Integer)
    date = Column(DateTime)
    groups = relationship("ArticleGroup")

class ArticleGroup(Base):
    __tablename__ = 'articlegroups'

    article = Column(String, ForeignKey('articles.id'), primary_key=True)
    group = Column(String, primary_key=True)
    number = Column(Integer)
