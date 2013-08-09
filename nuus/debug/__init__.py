from flask import Blueprint, url_for, render_template, request, redirect, session, g, abort, flash, current_app, Response

import gzip
import nuus
from nuus import usenet, usenet_pool
from nuus.database import engine, tables
from nuus.utils import parse_date, date_deltas
from nuus.views import get_user
from nuus.indexer import decoder
import nntplib
import os
import pickle
from pprint import pprint
import re
from sqlalchemy.sql import select, bindparam
import sys
import time

__all__ = ['blueprint']

blueprint = Blueprint('debug', __name__, url_prefix='/debug')

BLOCK_STORAGE_DIR = nuus.app.config.get('BLOCK_STORAGE_DIR')
BLOCK_FILE_REGEX = re.compile(nuus.app.config.get('BLOCK_FILE_REGEX'))

def get_all_files():
    groups = {}
    for fn in os.listdir(BLOCK_STORAGE_DIR) + os.listdir(os.path.join(BLOCK_STORAGE_DIR, 'complete')):
        m = BLOCK_FILE_REGEX.match(fn)
        if m:
            group = m.group('group')
            if not (group in groups):
                groups[group] = []
            groups[group].append((int(m.group('start')), fn))
    for group in groups:
        groups[group] = sorted(groups[group])
    return groups

@blueprint.route('/pages')
def list_pages():
    return render_template('debug/pages.html', groups=get_all_files(), user=get_user())

@blueprint.route('/nextfile')
def get_next_file():
    filename = request.args.get('filename')
    filter = request.args.get('filter')
    groups = get_all_files()
    m = BLOCK_FILE_REGEX.match(filename)
    group = m.group('group')
    start = int(m.group('start'))
    for n,fn in groups[group]:
        if n > start:
            return redirect(url_for('.dump_articles', filename=fn, filter=filter))
    return redirect(url_for('.list_pages'))

@blueprint.route('/prevfile')
def get_prev_file():
    filename = request.args.get('filename')
    filter = request.args.get('filter')
    groups = get_all_files()
    m = BLOCK_FILE_REGEX.match(filename)
    group = m.group('group')
    start = int(m.group('start'))
    prev = None
    for n,fn in groups[group]:
        if n >= start:
            return redirect(url_for('.dump_articles', filename=prev, filter=filter))
        prev = fn
    return redirect(url_for('.list_pages'))

@blueprint.route('/dump')
def dump_articles():
    filename = request.args.get('filename')
    group = request.args.get('group')
    filter = request.args.get('filter')
    if filename is None and group is None:
        flash('filename or group expected', 'danger')
        return redirect(url_for('.list_pages'))
    if filename is not None:
        files = [filename]
    else:
        groups = get_all_files()
        files = [fn for n,fn in groups[group]]
    articles = []
    for filename in files:
        fn = os.path.join(BLOCK_STORAGE_DIR, filename)
        if not os.path.exists(fn):
            fn = os.path.join(BLOCK_STORAGE_DIR, 'complete', filename)
            if not os.path.exists(fn):
                flash('file ' + filename + ' does not exist', 'danger')
                return redirect(url_for('.list_pages'))
        with gzip.open(fn, 'r') as f:
            articles += pickle.load(f)
    _articles = []
    while len(articles):
        n, a = articles.pop(0)
        a['subject'] = os.fsencode(a['subject']).decode('latin-1')
        if filter is None or filter in a['subject']:
            _articles.append(a)
        
    return render_template('debug/articles.html', filename=filename, group=None, filter=filter, articles=_articles, user=get_user())

@blueprint.route('/article')
def get_article():
    message_id = request.args.get('message_id')
    if message_id is None:
        flash('message_id required', 'danger')
        return redirect(request.referrer)
    # check cache
    cfn = os.path.join(BLOCK_STORAGE_DIR, 'cache', message_id[1:-1])
    #if os.path.exists(cfn):
    #    with gzip.open(cfn, 'r') as f:
    #        rval = f.read()
    #    return rval
    u = usenet.Usenet(connection_pool=usenet_pool)
    lines = u.get_article(message_id)
    rval = decoder.decode(lines)
    with gzip.open(cfn, 'w') as f:
        f.write(rval)
    return Response(rval, content_type='text/plain')
