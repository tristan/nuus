import cPickle as pickle
from flask import Blueprint, url_for, render_template, request, redirect, session, g, abort, flash
import gzip
from nntplib import NNTPError
from nuus import redis_pool, usenet_pool, models, usenet
from nuus.utils import rkey
import os
from redis import StrictRedis

__all__ = ['blueprint']

blueprint = Blueprint('nuus', __name__)

@blueprint.route('/')
def index():
    return render_template('index.html')

@blueprint.route('/r')
def releases():
    return "TODO"

@blueprint.route('/f')
def files():
    return "TODO"

@blueprint.route('/g', methods=["GET", "POST"])
def groups():
    redis = StrictRedis(connection_pool=redis_pool)
    if request.method == "POST":
        group = request.form.get('group')
        if group.startswith('a.b.'):
            group = 'alt.binaries.' + group[4:]
        if not redis.keys(rkey('group', group, 'last_article')):
            try:
                usenet = usenet.Usenet(connection_pool=usenet_pool)
                usenet.group_info(group)
                redis.set(rkey('group', group, 'last_article'), 0)
            except NNTPError as e:
                flash(e.message, 'error')
        else:
            flash('group %s already added' % group, 'error')            
        return redirect(url_for('.groups'))
    groups = map(lambda x: rkey.split(x)[1], redis.keys(rkey('group','*','last_article')))
    return render_template('groups.html', groups=groups)

@blueprint.route('/s')
def status():
    return "TODO"

@blueprint.route('/n')
def nzbs():
    return "TODO"

@blueprint.route('/c')
@blueprint.route('/c/<string:group>')
@blueprint.route('/c/<string:group>/<string:page>')
def cache(group=None,page=None):
    groups = os.listdir('cache')
    if group is not None and group in groups:
        caches = sorted(os.listdir(os.path.join('cache',group)))
        if page is not None and page in caches:
            f = gzip.open(os.path.join('cache',group,page), 'r')
            articles = pickle.load(f)
            f.close()
            return render_template('cache.html', cached_groups=groups, 
                                   group=group, caches=caches, page=page,
                                   articles=articles)
        return render_template('cache.html', cached_groups=groups, group=group, caches=caches)
    return render_template('cache.html', cached_groups=groups)
        
        
    
