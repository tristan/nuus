import os
import sys
import logging
from flask import Flask
import redis
from nuus import usenet

logger = logging.getLogger('nuus')

app = Flask(__name__,static_folder=os.path.join('..','static'),template_folder=os.path.join('..','templates'))
app.config.from_object('nuus.settings_defaults')
try:
    app.config.from_object('nuus.settings')
except:
    logger.error('error parsing settings.py')
    sys.exit(1)

# redis connection pool
redis_pool = redis.ConnectionPool(
    **{x[6:].lower(): app.config[x] for x in
       filter(lambda x: x.startswith('REDIS_'), app.config.keys())})

# usenet connection pool
usenet_pool = usenet.ConnectionPool(
    **{x[7:].lower(): app.config[x] for x in
       filter(lambda x: x.startswith('USENET_'), app.config.keys())})

def init_app():
    from .views import blueprint as main_blueprint
    app.register_blueprint(main_blueprint)
