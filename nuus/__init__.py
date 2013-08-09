import os
import sys
import logging
from flask import Flask
from nuus import usenet

logging.basicConfig()
logger = logging.getLogger('nuus')

app = Flask(__name__,static_folder=os.path.join('..','static'),template_folder=os.path.join('..','templates'))
app.config.from_object('nuus.settings_defaults')
try:
    app.config.from_object('nuus.settings')
except:
    logger.error('error parsing settings.py')
    sys.exit(1)

# usenet connection pool
usenet_pool = usenet.ConnectionPool(
    **{x[7:].lower(): app.config[x] for x in
       [x for x in list(app.config.keys()) if x.startswith('USENET_')]})

def init_app():
    from .views import blueprint as main_blueprint
    from .debug import blueprint as debug_blueprint
    app.register_blueprint(main_blueprint)
    app.register_blueprint(debug_blueprint)
