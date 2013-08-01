import cPickle as pickle
import gzip
from nuus.database import SimpleDatabase
from nuus.utils import parse_date
import os
import re
import shutil
import struct
import sys

def split_database(input_file,block_size=1000000):
    orig_db = SimpleDatabase(input_file)
    orig_pos = 0
    blocks = {}
    while True:
        result = orig_db._read_one(orig_pos)
        if result is None:
            break
        event, key, data, _, orig_pos = result
        if event != '\x00':
            continue
        # get block base
        block_key = (key / block_size) * block_size
        if not (block_key in blocks):
            block_db = SimpleDatabase('%s.%s' % (input_file, block_key))
            blocks[block_key] = block_db
        else:
            block_db = blocks[block_key]
        block_db._write_one(key, event, data)

"""
* sort db files
* check db for missing articles
* re-create tasks to check missing blocks
"""

def sort_database(input_file):
    orig_db = SimpleDatabase(input_file)
    sorted_file = '%s.%s' % (input_file, 'sorted')
    orig_db.compact(filename=sorted_file)
    orig_db.close()
    shutil.move(sorted_file, input_file)

if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    split = False
    sort = False
    dump = False
    check = True
    clean_utf = False

    if split:
        for db_filename in os.listdir('cache'):
            if re.match('^[a-z\.]+-article.db$', db_filename):
                split_database(os.path.join('cache', db_filename))

    if sort:
        for db_filename in os.listdir('cache'):
            if re.match('^[a-z\.]+-article.db\.\d+$', db_filename):
                print db_filename
                sort_database(os.path.join('cache',db_filename))

    if dump:
        done_dir = os.path.join('cache','done')
        src_dir = os.path.join('cache','old')
        max_lens = [0,0,0]
        for group in os.listdir(src_dir):
            for page in os.listdir(os.path.join(src_dir,group)):
                print group, page, max_lens
                page_file = os.path.join(src_dir,group,page)
                with gzip.open(page_file, 'r') as f:
                    articles = pickle.load(f)
                gmlines = []
                alines = []
                for number, subject, poster, sdate, id, _, size, _ in articles:
                    max_lens = map(max,zip(max_lens,[len(subject),len(poster),len(id)]))
                    try:
                        date = parse_date(sdate)
                    except:
                        if sdate == "":
                            date = 0
                        else:
                            print "FAILED TO PARSE DATE:", sdate
                            raise
                    alines.append('%s\n' % '\t'.join([id[1:-1], subject, poster, str(date), size]))
                    gmlines.append('%s\t%s\n' % (group, id[1:-1]))
                with gzip.open(os.path.join('cache','%s-bulk-articles.%s.gz' % (group, page)), 'w') as f:
                    f.writelines(alines)
                with gzip.open(os.path.join('cache','%s-bulk-group-mappings.%s.gz' % (group, page)), 'w') as f:
                    f.writelines(gmlines)
                shutil.move(page_file, os.path.join(done_dir, '%s.%s.gz' % (group, page)))
        print max_lens

    if check:
        src_dir = os.path.join('cache')
        for page in os.listdir(src_dir):
            print page
            if re.match('^alt\.binaries\.[^-]+-bulk-[\w-]+\.\d+\.gz$', page):
                page_file = os.path.join(src_dir,page)
                with gzip.open(page_file, 'r') as f:
                    for line in f.readlines():
                        try:
                            line.decode('utf-8')
                        except:
                            print line
                            raise

    if clean_utf:
        src_dir = os.path.join('cache')
        for page in os.listdir(src_dir):
            print page,
            if re.match('^alt\.binaries\.[^-]+-bulk-[\w-]+\.\d+\.gz$', page):
                print '... processing'
                page_file = os.path.join(src_dir, page)
                with gzip.open(page_file, 'r') as f:
                    lines = f.readlines()
                with gzip.open(page_file, 'w') as f:
                    f.writelines(map(lambda x: x.decode('latin-1').encode('utf-8'), lines))
