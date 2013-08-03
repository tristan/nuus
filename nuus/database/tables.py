from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint
from nuus.database import engine

metadata = MetaData()

### RELEASE TABLES

releases = Table(
    'releases', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(1024)),
    Column('poster', String(512)),
    Column('date', Integer),
    Column('parts', Integer),
    Column('nfo', Integer), # points to a file that is the nfo
    Column('nzb', Integer), # points to a file that is a nzb
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

files = Table(
    'files', metadata,
    Column('id', Integer, primary_key=True),
    Column('release_id', Integer),
    Column('name', String(1024)),
    Column('parts', Integer),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

file_groups = Table(
    'file_groups', metadata,
    Column('id', Integer, primary_key=True),
    Column('file_id', Integer, nullable=False),
    Column('group', String(64), nullable=False),
    UniqueConstraint('file_id', 'group', name='uix_1'),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

segments = Table(
    'segments', metadata,
    Column('id', Integer, primary_key=True),
    Column('file_id', Integer),
    Column('article_id', String(256), unique=True),
    Column('number', Integer),
    Column('size', Integer),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

#### INDEXER TABLES

tasks = Table(
    'tasks', metadata,
    Column('id', Integer, primary_key=True),
    Column('group', Integer, nullable=False),
    Column('start', Integer, nullable=False),
    Column('end', Integer, nullable=False),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

groups = Table(
    'groups', metadata,
    Column('id', Integer, primary_key=True),
    Column('group', String(64), nullable=False, unique=True),
    Column('last_post_checked', Integer, default=0),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

#### USER TABLE

users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True),
    Column('username', String(32), unique=True),
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
