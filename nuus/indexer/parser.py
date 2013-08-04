"""
(32572838,
  {':bytes': '787114',
   ':lines': '6096',
   'date': '04 Mar 2013 09:21:37 GMT',
   'from': 'Yenc@power-post.org (Yenc-PP-A&A)',
   'message-id': '<1362388896.91625.5@eu.news.astraweb.com>',
   'references': '',
   'subject': '130981[035/104] - "130981-0.4" yEnc (05/66)',
   'xref': 'headfeed.cambrium.nl alt.binaries.moovee:32572838 alt.binaries.hdtv.x264:214830450'})


to database Problems:

#1 releases

 * matched by unique 'name,poster'
 * id needed for files table
  - for insert and select

#2 files
 * matched by 'name,release_id'
 * id needed for segments
  - for insert only

#3 segments
 * matched by article_id (string)

 - assign file ids to segments
 - select article ids from db matching segments
 - create segments that don't exist

"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import gzip
import nuus
from nuus import usenet, usenet_pool
from nuus.database import engine, tables
from nuus.utils import parse_date, date_deltas
import os
import pickle
from pprint import pprint
import re
import shutil
from sqlalchemy.sql import select, bindparam
from sqlalchemy.exc import OperationalError
import sys
import time

BLOCK_STORAGE_DIR = nuus.app.config.get('BLOCK_STORAGE_DIR')
BLOCK_FILE_REGEX = re.compile(nuus.app.config.get('BLOCK_FILE_REGEX'))

with open('patterns.txt') as f:
    PATTERNS = [re.compile(p, re.I) for p in [x for x in [x.strip() for x in f.readlines()] if not (x == '' or x.startswith('#'))]]

def parse_articles(articles):
    """From the list of articles generate a map of releases->files->segments"""
    releases = dict()
    for n, article in articles:
        subject = os.fsencode(article['subject']).decode('latin-1')
        for pat in PATTERNS:
            rlsmatch = pat.match(subject)
            if rlsmatch:
                rlsmatch = rlsmatch.groupdict()
                date = parse_date(article['date'])
                size = int(article[':bytes'])
                poster = os.fsencode(article['from']).decode('latin-1')
                rlskey = (rlsmatch.get('release_name'), poster)
                if rlskey in releases:
                    release = releases.get(rlskey)
                else:
                    release = dict(date=date, total_parts=rlsmatch.get('release_total'), files={})
                    releases[rlskey] = release
                # get lowest date for the release
                if date < release['date']:
                    release['date'] = date
                if rlsmatch.get('file_name') in release['files']:
                    file = release['files'].get(rlsmatch.get('file_name'))
                else:
                    file = dict(segments=[], total_parts=rlsmatch.get('file_total'))
                    release['files'][rlsmatch.get('file_name')] = file
                file['segments'].append(dict(number=rlsmatch.get('file_part'),size=size,article_id=os.fsencode(article['message-id'][1:-1]).decode('latin-1')))
                break
    return releases

def to_database(group, releases):
    conn = engine.connect()
    releases_updates=[]
    release_groups_inserts=[]
    segments_inserts=[]
    new_release_count = new_file_count = new_segment_count = 0

    for rlskey in releases:
        release_name = rlskey[0]
        poster = rlskey[1]
        release = conn.execute(
            select([tables.releases]).where((tables.releases.c.name == release_name) & (tables.releases.c.poster == poster))
        ).fetchone()
        if release is None:
            # create it
            release = dict(name=release_name,poster=poster,size=0,date=releases[rlskey]['date'],
                           file_count=0,nzb_file_id=None,nfo_file_id=None,par2_file_count=0,archive_file_count=0,
                           total_parts=releases[rlskey]['total_parts'])
            r = conn.execute(tables.releases.insert(), **release)
            release['id'] = r.lastrowid
            new_release_count += 1
        else:
            release = dict(release)
            release['date'] = min(release['date'], releases[rlskey]['date'])
        for file_name in releases[rlskey]['files']:
            file = conn.execute(
                select([tables.files]).where((tables.files.c.name == file_name) & (tables.files.c.release_id == release['id']))
            ).fetchone()
            if file is None:
                # create it
                file = dict(name=file_name,release_id=release['id'],total_parts=releases[rlskey]['files'][file_name]['total_parts'])
                r = conn.execute(tables.files.insert(), **file)
                file['id'] = r.lastrowid
                # update counts for the release
                release['file_count'] += 1
                fn = file_name.lower()
                # match archives
                if fn.endswith('.rar') or re.match('^.+\.r\d\d$', fn) or re.match('^.+\.\d\d\d$', fn):
                    release['archive_file_count'] += 1
                elif fn.endswith('.par2'):
                    release['par2_file_count'] += 1
                elif fn.endswith('.nfo'):
                    release['nfo_file_id'] = file['id']
                elif fn.endswith('.nzb'):
                    release['nzb_file_id'] = file['id']
                new_file_count += 1
            else:
                file = dict(file)
            segments = []
            for segment in releases[rlskey]['files'][file_name]['segments']:
                seg = conn.execute(
                    select([tables.segments]).where(tables.segments.c.article_id == segment['article_id'])
                ).fetchone()
                if seg is None:
                    segment['file_id'] = file['id']
                    release['size'] += segment['size']
                    segments.append(segment)
                    new_segment_count += 1
            if len(segments):
                conn.execute(tables.segments.insert(), segments)
        if not conn.execute(
                select([tables.release_groups]).where((tables.release_groups.c.release_id == release['id']) & (tables.release_groups.c.group == group))
        ).fetchone():
            conn.execute(tables.release_groups.insert(),release_id=release['id'],group=group)
        conn.execute(tables.releases.update().where(tables.releases.c.id == release['id']).values(
            size=release['size'],date=release['date'],file_count=release['file_count'],
            nzb_file_id=release['nzb_file_id'],nfo_file_id=release['nfo_file_id'],
            par2_file_count=release['par2_file_count'],archive_file_count=release['archive_file_count']
        ))

    return (new_release_count, new_file_count, new_segment_count)

def beta_to_database(group, releases):
    rlssel = None
    for rlskey in releases:
        release_name = rlskey[0]
        poster = rlskey[1]
        s = (tables.releases.c.name == release_name) & (tables.releases.c.poster == poster)
        rlssel = (rlssel | s) if rlssel else s
    for row in conn.execute(select([tables.releases]).where(rlssel)):
        rlskey = (row['name'],row['poster'])
        rls = dict(row)
        rls['date'] = min(rls['date'], releases[rlskey]['date'])
        releases[rlskey].update(rls)
    values = [r for r in releases.values() if not ('id' in r)]
    rid = conn.execute(tables.releases.insert(), values).lastrowid
    for r in values:
        r.update(dict(
            id=rid,size=0,file_count=0,nzb_file_id=None,nfo_file_id=None,
            par2_file_count=0,archive_file_count=0))
        rid += 1
    fsel = None
    for rls in releases.values():
        for fn in rls['files']:
            s = (tables.files.c.release_id == rls['id']) & (tables.files.c.name == fn)
            fsel = (fsel | s) if fsel else s

if __name__ == '__main__':
    group = sys.argv[1]
    if group.startswith('a.b.'):
        group = 'alt.binaries.' + group[4:]
    for fn in os.listdir(BLOCK_STORAGE_DIR):
        m = BLOCK_FILE_REGEX.match(fn)
        if not m or m.group('group') != group:
            continue
        print(fn, end=' ')
        sys.stdout.flush()
        start_time = time.time()
        with gzip.open(os.path.join(BLOCK_STORAGE_DIR, fn), 'r') as f:
            articles = pickle.load(f)
        releases = parse_articles(articles)
        while True:
            try:
                nr, nf, ns = to_database(group, releases)
                print('releases: %d, files: %d, segments: %d' % (nr, nf, ns), end=' ')
            except OperationalError as e:
                print(e)
                print('retrying')
                continue
            break
        shutil.move(os.path.join(BLOCK_STORAGE_DIR, fn), os.path.join(BLOCK_STORAGE_DIR, 'complete', fn))
        print('took: %dm%ds' % date_deltas(time.time() - start_time)[2:])
