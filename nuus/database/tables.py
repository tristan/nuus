from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from nuus.database import engine

metadata = MetaData()

articles = Table(
    'articles', metadata,
    Column('id', String(256), primary_key=True),
    Column('subject', String(4096)),
    Column('poster', String(512)),
    Column('date', Integer),
    Column('size', Integer),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

group_article_map = Table(
    'group_article_map', metadata,
    Column('group', String(64), primary_key=True),
    Column('article', String(256), primary_key=True),
    mysql_engine='MyISAM',
    mysql_charset='utf8'
)

def create_tables():
    metadata.create_all(engine)

if __name__ == '__main__':
    create_tables()
