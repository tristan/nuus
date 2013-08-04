from flask import Blueprint, url_for, render_template, request, redirect, session, g, abort, flash, current_app, Response
import md5
from nuus.utils import rkey, humanize_date_difference
from nuus.database import engine, tables
import os
import re
from sqlalchemy.sql import select, or_, func

__all__ = ['blueprint']

blueprint = Blueprint('nuus', __name__)

def get_user(user_id=None):
    if user_id is None:
        user_id = session.get('user_id')
    if user_id is None:
        return None
    conn = engine.connect()
    sel = select([tables.users]).where(tables.users.c.id == user_id)
    user = conn.execute(sel).fetchone()
    conn.close()
    return user

def md5hash(s):
    m = md5.new()
    m.update(s)
    return m.hexdigest()

@blueprint.route('/logout')
def logout():
    session.pop('user_id')
    return redirect(url_for('.index'))

@blueprint.route('/', methods=['GET','POST'])
def index():
    query = request.args.get('query')
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    rd_offset = offset
    rd_limit = limit
    offset = int(offset) if offset else 0
    limit = int(limit) if limit else 25
    user = get_user()
    if request.method == 'POST':
        username = request.form.get('username')
        if username is not None:
            password = md5hash(request.form.get('password', ''))
            conn = engine.connect()
            sel = select([tables.users]).where(tables.users.c.username == username)
            user = conn.execute(sel).fetchone()
            if user is None and username in current_app.config.get('VALID_USERS'):
                conn.execute(tables.users.insert(), username=username,password=password)
                user = conn.execute(sel).fetchone()
            elif user is not None and user['password'] != password:
                user = None
            if user is not None:
                session['user_id'] = user['id']
            else:
                flash(u'invalid username or password', 'danger')
            conn.close()
        sabnzbd_host = request.form.get('sabnzbd_host')
        if sabnzbd_host:
            sabnzbd_apikey = request.form.get('sabnzbd_apikey')
            conn = engine.connect()
            conn.execute(tables.users.update().values(dict(
                sabnzbd_host=sabnzbd_host,
                sabnzbd_apikey=sabnzbd_apikey
            )))
            conn.close()
        return redirect(url_for('.index', query=query,offset=rd_offset,limit=rd_limit))
    user = get_user()
    releases = []
    total_results = 0
    if session.has_key('user_id') and query:
        conn = engine.connect()
        sel = select([tables.releases]).where(tables.releases.c.name.like('%%%s%%' % query)).order_by(-tables.releases.c.date)
        res = conn.execute(sel)
        total_results = res.rowcount
        res.cursor.rownumber += offset
        for row in res.fetchmany(limit):
            release = dict(row)
            if release['file_count'] == 0: # hasn't been sized yet, use old dets to fill
                sel = select([tables.files]).where(tables.files.c.release_id == row['id'])
                file_count = 0
                par2_count = 0
                archive_count = 0
                nfo_file = None
                nzb_file = None
                size = 0
                _seg_where = []
                _fg_where = []
                for row_ in conn.execute(sel):
                    # extend query for segments 
                    _seg_where.append(tables.segments.c.file_id == row_['id'])
                    _fg_where.append(tables.file_groups.c.file_id == row_['id'])
                    file_count += 1
                    fn = row_['name'].lower()
                    if fn.endswith('.par2'):
                        par2_count += 1
                    elif fn.endswith('.rar') or re.match('^.+\.r\d\d$', fn) or re.match('^.+\.\d\d\d$', fn):
                        archive_count += 1
                    elif fn.endswith('.nfo'):
                        nfo_file = row_['id']
                    elif fn.endswith('nzb'):
                        nzb_file = row_['id']
                # TODO: this is super quick after indexing
                sel = select([tables.segments]).where(or_(*_seg_where))
                for row_ in conn.execute(sel):
                    size += row_['size']
                #release['size'] = "UNKNOWN"
                release['size'] = size
                sel = select([tables.file_groups.c.group]).where(or_(*_fg_where)).distinct()
                groups = map(lambda x: x[0], conn.execute(sel).fetchall())
                release['groups'] = groups
                release['file_count'] = file_count
                release['archive_file_count'] = archive_count
                release['par2_file_count'] = par2_count
                if nfo_file:
                    release['nfo'] = nfo_file
                if nzb_file:
                    release['nzb'] = nzb_file
            else:
                sel = select([tables.release_groups.c.group]).where(tables.release_groups.c.release_id == release['id']).distinct()
                groups = map(lambda x: x[0], conn.execute(sel).fetchall())
                release['groups'] = groups
            age = humanize_date_difference(release['date'])
            release['age'] = age
            releases.append(release)
        res.close()
        conn.close()
    return render_template('index.html', user=user, query=query, results=releases, total_results=total_results, offset=offset, limit=limit)


nzb = """<?xml version="1.0" encoding="iso-8859-1" ?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
<head>
<meta type="title">{release_name}</meta>
</head>
{files}
</nzb>
"""

nzb_file = """<file poster="{poster}" date="{date}" subject="{subject}">
<groups>
{groups}
</groups>
<segments>
{segments}
</segments>
</file>
"""

nzb_group = """<group>{group}</group>"""

nzb_segment = """<segment bytes="{size}" number="{number}">{article_id}</segment>"""

@blueprint.route('/nzb/<int:release_id>.nzb')
def get_nzb(release_id):
    conn = engine.connect()
    sel = select([tables.releases]).where(tables.releases.c.id == release_id)
    release = conn.execute(sel).fetchone()
    files_part = ""
    sel = select([tables.files]).where(tables.files.c.release_id == release_id)
    for f in conn.execute(sel):
        sel = select([tables.segments]).where(tables.segments.c.file_id == f['id']).order_by(tables.segments.c.number)
        segments_parts = []
        for s in conn.execute(sel):
            segments_parts.append(nzb_segment.format(size=s['size'],number=s['number'],article_id=s['article_id']))
        segments_part = '\n'.join(segments_parts)
        sel = select([tables.file_groups.c.group]).where(tables.file_groups.c.file_id == f['id']).distinct()
        groups_part = '\n'.join(map(lambda x: nzb_group.format(group=x[0]), conn.execute(sel).fetchall()))
        files_part += nzb_file.format(poster=release['poster'], date=release['date'], subject=f['name'],
                                      groups=groups_part,segments=segments_part)
    return Response(nzb.format(release_name=release['name'],files=files_part), mimetype='application/x-nzb')
    # http://wiki.sabnzbd.org/api#toc28
    # http://miffy:9095/api?mode=addurl&apikey=d96490fab5f6e61b8fafd66e50886c26&name=http://localhost:5000/nzb/1000.nzb&nzbname=test
