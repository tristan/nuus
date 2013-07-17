from flask import Blueprint, url_for, render_template, request, redirect, session, g, abort, flash
from nuus import redis_pool, usenet_pool, models, usenet
from redis import StrictRedis
from nntplib import NNTPError

__all__ = ['blueprint']

blueprint = Blueprint('nuus', __name__)

@blueprint.route('/')
def index():
    return render_template('index.html')

@blueprint.route('/r')
def releases():
    redis = StrictRedis(connection_pool=redis_pool)
    keys = redis.keys('release:*:files')
    releases = []
    for k in keys:
        _,subj,_ = k.split(':')
        try:
            # TODO: figure out unicode shite
            unicode(subj)
        except:
            continue
        releases.append(models.ReleaseWrapper(redis, subj))
    return render_template('releases.html', releases=releases)

@blueprint.route('/f')
def files():
    redis = StrictRedis(connection_pool=redis_pool)
    keys = redis.keys('file:*:filename')
    files = []
    for k in keys:
        _,subj,_ = k.split(':')
        try:
            # TODO: figure out unicode shite
            unicode(subj)
        except:
            continue
        files.append(models.FileWrapper(redis, subj))
    return render_template('files.html', files=files)

@blueprint.route('/g', methods=["GET", "POST"])
def groups():
    redis = StrictRedis(connection_pool=redis_pool)
    if request.method == "POST":
        group = request.form.get('group')
        if group.startswith('a.b.'):
            group = 'alt.binaries.' + group[4:]
        try:
            usenet = usenet.Usenet(connection_pool=usenet_pool)
            usenet.group_info(group)
            redis.sadd('groups', group)
        except NNTPError as e:
            flash(e.message, 'error')
        return redirect(url_for('.groups'))
    groups = redis.smembers('groups')
    return render_template('groups.html', groups=groups)

@blueprint.route('/s')
def status():
    redis = StrictRedis(connection_pool=redis_pool)
    groups = redis.smembers('groups')
    groups = [models.GroupWrapper(redis, g) for g in groups]
    return render_template('status.html', progress=True, groups=groups)

@blueprint.route('/n')
def nzbs():
    redis = StrictRedis(connection_pool=redis_pool)
    keys = redis.keys('file:*.nzb*:groups')
    files = []
    for k in keys:
        _,subj,_ = k.split(':')
        try:
            # TODO: figure out unicode shite
            unicode(subj)
        except:
            continue
        files.append(models.FileWrapper(redis, subj))
    return render_template('nzbs.html', nzbs=files)
