from flask import Blueprint, url_for, render_template, request, redirect, session, g, abort, flash, current_app, Response
from hashlib import md5
from nuus.utils import humanize_date_difference
from nuus.database import engine, tables
from nuus import nzb
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
    m = md5()
    m.update(s.encode())
    return m.hexdigest()

def humanize_size(s):
    p = ['bytes', 'KB','MB','GB','TB']
    while s > 1024 and len(p) > 1:
        s /= 1024
        p = p[1:]
    return '{0:.2f} {1}'.format(s, p[0])

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
                flash('invalid username or password', 'danger')
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
    if 'user_id' in session and query:
        conn = engine.connect()
        total_results = conn.execute(select([func.count(),tables.releases]).where(tables.releases.c.name.like('%%%s%%' % query))).scalar()
        sel = select([tables.releases]).where(tables.releases.c.name.like('%%%s%%' % query)).order_by(-tables.releases.c.date).limit(limit).offset(offset)
        res = conn.execute(sel)
        for row in res.fetchall():
            release = dict(row)
            sel = select([tables.release_groups.c.group]).where(tables.release_groups.c.release_id == release['id']).distinct()
            groups = [x[0] for x in conn.execute(sel).fetchall()]
            release['groups'] = groups
            age = humanize_date_difference(release['date'])
            release['age'] = age
            release['size'] = humanize_size(release['size'])
            releases.append(release)
        res.close()
        conn.close()
    return render_template('index.html', user=user, query=query, results=releases, total_results=total_results, offset=offset, limit=limit)


@blueprint.route('/nzb/<int:release_id>.nzb')
def get_nzb(release_id):
    return Response(nzb.create_nzb(release_id), mimetype='application/x-nzb')
    # http://wiki.sabnzbd.org/api#toc28
    # http://miffy:9095/api?mode=addurl&apikey=d96490fab5f6e61b8fafd66e50886c26&name=http://localhost:5000/nzb/1000.nzb&nzbname=test
