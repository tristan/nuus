from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint, Index
from sqlalchemy.dialects.mysql import BIGINT
from nuus.database import engine

metadata = MetaData()

### RELEASE TABLES

releases = Table(
    'releases', metadata,
    Column('id', Integer, primary_key=True),
    # 1000 is max length that can be indexed
    Column('name', String(1000), index=True, nullable=False),
    Column('poster', String(512)),
    Column('date', Integer),
    Column('total_parts', Integer),
    Column('nfo_file_id', Integer), # points to a file that is the nfo
    Column('nzb_file_id', Integer), # points to a file that is a nzb
    Column('file_count', Integer, default=0),
    Column('archive_file_count', Integer, default=0),
    Column('par2_file_count', Integer, default=0),
    Column('size', BIGINT, default=0),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

files = Table(
    'files', metadata,
    Column('id', Integer, primary_key=True),
    Column('release_id', Integer, index=True, nullable=False),
    Column('name', String(1024), index=True, nullable=False),
    Column('total_parts', Integer),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

release_groups = Table(
    'release_groups', metadata,
    Column('id', Integer, primary_key=True),
    Column('release_id', Integer, nullable=False),
    Column('group', String(64), nullable=False),
    UniqueConstraint('release_id', 'group'),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

segments = Table(
    'segments', metadata,
    Column('id', Integer, primary_key=True),
    Column('file_id', Integer, index=True, nullable=False),
    Column('article_id', String(256), unique=True, index=True, nullable=False),
    Column('number', Integer, nullable=False),
    Column('size', Integer, default=0),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

#### INDEXER TABLES

groups = Table(
    'groups', metadata,
    Column('id', Integer, primary_key=True),
    Column('group', String(64), nullable=False, unique=True),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

#### USER TABLE

users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('username', String(32), unique=True, index=True),
    Column('password', String(32), nullable=False),
    Column('sabnzbd_host', String(256), default='http://127.0.0.1:8080/'),
    Column('sabnzbd_apikey', String(32)),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

def create_tables():
    metadata.create_all(engine)

if __name__ == '__main__':
    #metadata.drop_all(engine)
    create_tables()
