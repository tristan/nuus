import gzip
import nuus
from nuus.database import engine, tables
import os
import re
import shutil
import subprocess
import sys
import tempfile
from nuus.indexer.workers import NEW_ARTICLES_DIR

def mysqlimport(in_fns, temp_fn):
    with open(temp_fn, 'w') as f:
        sys.stdout.write('>')
        subprocess.call(['zcat'] + in_fns, stdout=f)
    sys.stdout.write('i')
    subprocess.call(['mysqlimport','-u',nuus.app.config.get('DATABASE_USER', 'mysql'),'--local',nuus.app.config.get('DATABASE_DATABASE', 'nuus'), temp_fn])
    sys.stdout.write('m')
    subprocess.call(['mv'] + in_fns + [os.path.abspath(os.path.join(NEW_ARTICLES_DIR, '..', 'done'))])

def bulk_import():
    """
    1. zcat parts > /tmp/articles.dat
    2. mysqlimport --local -u {user} {name} /tmp/articles.dat
    3. mv parts > ../done/parts

    TODO: 10000 articles per part doesn't give mysqlimport much to do
    """
    temp_articles_fn = os.path.join(tempfile.gettempdir(), 'articles.dat')
    temp_group_map_fn = os.path.join(tempfile.gettempdir(), 'group_article_map.dat')
    article_in_files = []
    group_in_files = []
    for fn in os.listdir(NEW_ARTICLES_DIR):
        fullfn = os.path.join(NEW_ARTICLES_DIR, fn)
        sys.stdout.write('.')
        if 'articles' in fn:
            article_in_files.append(fullfn)
            if len(article_in_files) >= 10:
                mysqlimport(article_in_files, temp_articles_fn)
                article_in_files = []
        elif 'group' in fn:
            group_in_files.append(fullfn)
            if len(group_in_files) >= 100:
                mysqlimport(group_in_files, temp_group_map_fn)
                group_in_files = []
        else:
            print('unhandled file:', fn)
            continue
    if len(article_in_files):
        mysqlimport(article_in_files, temp_articles_fn)
    if len(group_in_files):
        mysqlimport(group_in_files, temp_group_map_fn)
                                         

if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    bulk_import()
