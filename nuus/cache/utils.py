import cPickle as pickle
import gzip
from nuus.database import SimpleDatabase
import os
import shutil

def _get_group_cache_file(group):
    return os.path.join('cache', '%s-article.db' % group)

def _open_cache_db(group):
    db_file = _get_group_cache_file(group)
    exists = os.path.exists(db_file)
    db = SimpleDatabase(db_file)
    return (db, not exists)

def old_cache_to_new_cache(group, db=None):
    if db is None:
        db, created = _open_cache_db(group)
        if created:
            return []
    else:
        created = False
    print 'processing old cache for group:', group
    index = {}
    if not created:
        index = db._index()
    for page in os.listdir(os.path.join('cache',group)):
        print page,
        pf = os.path.join('cache',group,page)
        f = gzip.open(pf, 'r')
        try:
            articles = pickle.load(f)
            f.close()
            for a in articles:
                num = int(a[0])
                art = a[1:]
                if num not in index:
                    db.set(num, art)
        except IOError as e:
            f.close()
            if e.message.startswith('CRC check failed'):
                print '\nCRC check failed on:', pf
                os.remove(pf)

def compact_cache(group):
    db, created = _open_cache_db(group)
    cfn = _get_group_cache_file(group) + '.compacted'
    if created:
        return
    db.compact(filename=cfn)
    db.close()
    shutil.move(cfn, db._filename)

def cached_ranges(group=None, db=None):
    if db is None and group is not None:
        db, created = _open_cache_db(group)
        if created:
            return []
    elif group is None and db is None:
        raise 'Requires `group` or `db` input'
    idx = db._index()
    L = sorted(idx.keys())
    G = [list(x) for _,x in groupby(L, lambda x,c=count(): next(c)-x)]
    return [(x[0], x[-1]) for x in G]
