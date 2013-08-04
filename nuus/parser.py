import chardet
import codecs
import copy
import gzip
from nuus import utils
from nuus.database import engine
from nuus.database.tables import releases, files, file_groups, segments
import nuus
import os
import re
import shutil
from sqlalchemy.sql import select, and_, or_, not_
#from sqlalchemy.sql.expression import collate
import sys
import time

CACHE_BASE = nuus.app.config.get('CACHE_BASE')
CACHE_INBOX = nuus.app.config.get('CACHE_INBOX')
CACHE_SKIPPED = nuus.app.config.get('CACHE_SKIPPED')
CACHE_COMPLETE = nuus.app.config.get('CACHE_COMPLETE')

CACHE_FILE_FORMAT = nuus.app.config.get('CACHE_FILE_FORMAT')
CACHE_FILE_REGEX = re.compile(nuus.app.config.get('CACHE_FILE_REGEX'))
CACHE_LINE_FORMAT = nuus.app.config.get('CACHE_LINE_FORMAT')
CACHE_LINE_REGEX = re.compile(nuus.app.config.get('CACHE_LINE_REGEX'))


class Parser(object):
    def __init__(self, patterns):
        self._patterns = patterns

    def run(self):
        print CACHE_FILE_REGEX.pattern
        for filename in os.listdir(CACHE_INBOX):
            self.parse_file(filename)
            shutil.move(os.path.abspath(os.path.join(CACHE_INBOX, filename)),
                        os.path.abspath(os.path.join(CACHE_COMPLETE, filename)))

    def parse_file(self, filename):
        m_filename = CACHE_FILE_REGEX.match(filename)
        if m_filename and m_filename.group('status') == 'new':
            group = m_filename.group('group')
            page = m_filename.group('page')
            print 'parsing group: %s, page: %s' % (group, page),
            start_time = time.time()
            skipped_count = 0
            matched_count = 0
            in_fp = os.path.join(CACHE_INBOX, filename)
            matches = []
            sk_fp = os.path.join(CACHE_SKIPPED, CACHE_FILE_FORMAT.format(group=group,page=page,status='skipped'))
            with gzip.open(in_fp, 'r') as f, gzip.open(sk_fp, 'w') as skipped_out:
                while True:
                    line = f.readline()
                    if line == '':
                        break # eof reached
                    decodedline = line.decode('utf-8')
                    decodedline = line
                    matched = self.parse_line(group, decodedline)
                    if matched:
                        matches.append(matched)
                        matched_count += 1
                    else:
                        skipped_out.write(line)
                        skipped_count += 1
            if matched_count > 0:
                self.process_matches(matches)
            print 'matched: %s, skipped: %s, time: %s' % (matched_count, skipped_count, utils.time_taken_to_str(time.time() - start_time))

    def parse_line(self, group, line):
        m = CACHE_LINE_REGEX.match(line)
        if not m:
            return None
        subject = m.group('subject')
        for pat in self._patterns:
            n = pat.match(subject)
            if n:
                return dict([('group',group)] + m.groupdict().items() + n.groupdict().items())
        return None

    def process_matches(self, matches):
        _releases = dict()
        _files = dict()
        _fgs = set()
        _segs = dict()
        mkkey = lambda t: tuple(map(lambda x: str(x).lower(), t))
        for m in matches:
            release, f, fg, s = self.process_match(**m)
            _releases[mkkey((release['name'],release['poster']))] = release
            _files[mkkey((release['name'],release['poster'],f['name']))] = f
            _fgs.add(mkkey((release['name'],release['poster'],f['name'],fg['group'])))
            _segs[mkkey((release['name'],release['poster'],f['name'],s['article_id']))] = s
        conn = engine.connect()
        if not _releases:
            raise Exception('releases is empty!!!!')
            return
        from pprint import pprint
        #pprint(_releases)
        new_releases = copy.copy(_releases)
        old_releases = copy.copy(_releases)
        _releases = dict()
        # get all existing releases
        s1 = reduce(lambda a,x: a + (and_(releases.c.name == x[0], releases.c.poster == x[1]),), 
                    old_releases.keys(), tuple())
        s = select([releases]).where(or_(*s1))
        results = conn.execute(s).fetchall()
        for row in results:
            key = mkkey((row['name'], row['poster']))
            _releases[key] = dict(row)
            try:
                del new_releases[key]
            except:
                print '\nDUPLICATE FOUND: %s' % row
        if new_releases:
            # create new releases
            new_releases = new_releases.values()
            print 'new releases:', len(new_releases),
            rid = conn.execute(releases.insert(), new_releases).lastrowid
            # assign ids (i abuse .lastrowid so i don't have to query the db again)
            for r in new_releases:
                key = mkkey((r['name'], r['poster']))
                r['id'] = rid
                _releases[key] = r
                rid += 1
            new_releases = None

        # fill in release_id for files
        s = []
        for key in _files.keys():
            f = _files[key]
            f['release_id'] = _releases[key[:2]]['id']
            s.append(and_(files.c.release_id == f['release_id'], files.c.name == f['name']))
        # re-index releases
        _releases = {r['id']: r for r in _releases.values()}

        new_files = copy.copy(_files)
        old_files = copy.copy(_files)
        _files = dict()
        # get existing files
        s = select([files]).where(or_(*s))
        results = conn.execute(s).fetchall()
        for row in results:
            key = mkkey((_releases[row['release_id']]['name'], _releases[row['release_id']]['poster'], row['name']))
            _files[key] = dict(row)
            del new_files[key]
        if new_files:
            new_files = new_files.values()
            print 'new files:', len(new_files),
            rid = conn.execute(files.insert(), new_files).lastrowid
            for f in new_files:
                key = mkkey((_releases[f['release_id']]['name'], _releases[f['release_id']]['poster'], f['name']))
                f['id'] = rid
                _files[key] = f
                rid += 1
            new_files = None

        # fill in file_id for file_groups
        new_fgs = set()
        s = []
        for key in _fgs:
            group = key[-1]
            file_id = _files[key[:3]]['id']
            s.append(and_(file_groups.c.file_id == file_id, file_groups.c.group == group))
            new_fgs.add(mkkey((file_id,group)))
        # get existing file_groups
        s = select([file_groups]).where(or_(*s))
        results = conn.execute(s).fetchall()
        for row in results:
            key = mkkey((row['file_id'],row['group']))
            try:
                new_fgs.remove(key)
            except:
                pprint(results)
                pprint(_fgs)
                raise

        # fill in file_id for segments
        s = []
        for key in _segs.keys():
            _segs[key]['file_id'] = _files[key[:3]]['id']
            s.append(segments.c.article_id == _segs[key]['article_id'])
        # re-index segments
        _segs = {s['article_id']: s for s in _segs.values()}
        # get existing segments
        s = select([segments]).where(or_(*s))
        for row in conn.execute(s):
            del _segs[row['article_id']]

        # transactional insert for fgs and segs
        with conn.begin() as trans:
            if _segs:
                print 'new segments:', len(_segs),
                conn.execute(segments.insert(), _segs.values())
            if new_fgs:
                conn.execute(file_groups.insert(), [dict(file_id=x[0],group=x[1]) for x in new_fgs])
            

    def process_match(self, group, article_id, poster, date, size,
                      file_name, file_part, file_total,
                      release_name, release_part=None, release_total=None, 
                      **unused):
        release_total = int(release_total) if release_total else -1
        release_part =  int(release_part) if release_part else None
        date = int(date)
        size = int(size)
        file_part = int(file_part)
        file_total = int(file_total)
        #release_name = release_name.decode(chardet.detect(release_name)['encoding'])
        #file_name = file_name.decode(chardet.detect(file_name)['encoding'])
        #poster = poster.decode(chardet.detect(poster)['encoding'])
        #article_id = article_id.decode(chardet.detect(article_id)['encoding'])

        release = dict(name=release_name,
                       poster=poster,
                       date=date,
                       parts=release_total)
        f = dict(name=file_name,
                 parts=file_total)
        fg = dict(group=group)
        seg = dict(article_id=article_id,
                   number=file_part,
                   size=size)
        return release, f, fg, seg
        

    def _old_process_match(self, **kwargs):
        conn = engine.connect()
        # process defaults
        release_total = int(release_total) if release_total else -1
        release_part =  int(release_part) if release_part else None
        date = int(date)
        size = int(size)
        file_part = int(file_part)
        file_total = int(file_total)

        # find existing release (or create)
        s = select([releases]).where(and_(
            releases.c.name == release_name, 
            releases.c.poster == poster,
            releases.c.parts == release_total
        ))
        rval = conn.execute(s)
        release = rval.fetchone()
        rval.close()
        if release is None:
            # create a new release
            release = dict(name=release_name,
                           poster=poster,
                           date=date,
                           parts=release_total)
            rval = conn.execute(releases.insert(), **release)
            release['id'] = rval.inserted_primary_key[0]
            rval.close()
        
        # find existing file (or create)
        s = select([files]).where(and_(
            files.c.release_id == release['id'],
            files.c.name == file_name,
            files.c.parts == file_total
        ))
        rval = conn.execute(s)
        f = rval.fetchone()
        rval.close()
        if f is None:
            f = dict(release_id=release['id'],
                     name=file_name,
                     parts=file_total)
            rval = conn.execute(files.insert(), **f)
            f['id'] = rval.inserted_primary_key[0]
            rval.close()
            with conn.begin():
                # add nfo or nzb to the release if required
                if (not release.has_key('nfo')) and file_name.endswith('.nfo'):
                    conn.execute(releases.update().where(releases.c.id == release['id']).values(nfo=f['id'])).close()
                elif (not release.has_key('nzb')) and file_name.endswith('.nzb'):
                    conn.execute(releases.update().where(releases.c.id == release['id']).values(nzb=f['id'])).close()
                # add group to file (shouldn't be a duplicate if the file has just been created)
                conn.execute(file_groups.insert(), 
                             file_id=f['id'],group=group)
                # add segment (shouldn't be a duplicate if the file has just been created)
                conn.execute(
                    segments.insert(),
                    file_id=f['id'],article_id=article_id,number=file_part, size=size)
        else:
            s = select([file_groups]).where(and_(
                file_groups.c.file_id == f['id'],
                file_groups.c.group == group,
            ))
            rval = conn.execute(s)
            fg = rval.fetchone()
            rval.close()
            if fg is None:
                conn.execute(
                    file_groups.insert(), 
                    file_id=f['id'],group=group
                ).close()
            s = select([segments]).where(segments.c.article_id == article_id)
            rval = conn.execute(s)
            seg = rval.fetchone()
            rval.close()
            if seg is None:
                conn.execute(
                    segments.insert(),
                    file_id=f['id'],article_id=article_id,number=file_part, size=size
                ).close()

if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    f = open('patterns.txt')
    patterns = [re.compile(p, re.I) for p in filter(lambda x: not (x == '' or x.startswith('#')), [x.strip() for x in f.readlines()])]
    f.close()
    parser = Parser(patterns)
    parser.run()
