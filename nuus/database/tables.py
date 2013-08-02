from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint
from nuus.database import engine

metadata = MetaData()

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

def create_tables():
    metadata.create_all(engine)

if __name__ == '__main__':
    metadata.drop_all(engine)
    create_tables()
