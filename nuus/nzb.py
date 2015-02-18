from nuus.database import engine, tables
from sqlalchemy.sql import select, text

NZB = """<?xml version="1.0" encoding="iso-8859-1" ?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<head>
<meta type="title">{release_name}</meta>
</head>
{files}
</nzb>
"""

NZB_FILE = """<file poster="{poster}" date="{date}" subject="{subject}">
<groups>
{groups}
</groups>
<segments>
{segments}
</segments>
</file>
"""

NZB_GROUP = """<group>{group}</group>"""

NZB_SEGMENT = """<segment bytes="{size}" number="{number}">{article_id}</segment>"""

def create_nzb(release_id):
    conn = engine.connect()
    sel = select([tables.releases]).where(tables.releases.c.id == release_id)
    release = conn.execute(sel).fetchone()
    files_part = ""
    sel = select([tables.release_groups.c.group]).where(tables.release_groups.c.release_id == release['id']).distinct()
    groups_part = '\n'.join([NZB_GROUP.format(group=x[0]) for x in conn.execute(sel).fetchall()])
    sel = select([tables.files]).where(tables.files.c.release_id == release_id)
    for f in conn.execute(sel):
        sel = select([tables.segments]).where(tables.segments.c.file_id == f['id']).order_by(tables.segments.c.number)
        segments_parts = []
        for s in conn.execute(sel):
            segments_parts.append(NZB_SEGMENT.format(size=s['size'],number=s['number'],article_id=s['article_id']))
        segments_part = '\n'.join(segments_parts)
        files_part += NZB_FILE.format(poster=release['poster'], date=release['date'], subject=f['name'],
                                      groups=groups_part,segments=segments_part)
    conn.close()
    return NZB.format(release_name=release['name'],files=files_part)

def is_release_complete(release_id):
    conn = engine.connect()
    for row in conn.execute(
            text("select releases.total_parts, count(*) from releases join files on files.release_id = releases.id and releases.id = :release_id group by files.release_id;"),
            release_id=release_id).fetchall():
        if row[0] != row[1]:
            conn.close()
            return False
    parts = 0
    tp = 0
    for row in conn.execute(
            text("select files.total_parts, count(*) from files join segments on files.id = segments.file_id and files.release_id = :release_id group by segments.file_id;"),
            release_id=release_id).fetchall():
        #if row[0] != row[1]:
        #    conn.close()
        #    return False        
        tp += int(row[0])
        parts += int(row[1])
    conn.close()
    # return true if we have at least 80% of the parts
    return (parts * 1.0) / tp > 0.8
