import nuus
from sqlalchemy import create_engine

config = {x[9:].lower(): nuus.app.config[x] for x in [x for x in list(nuus.app.config.keys()) if x.startswith('DATABASE_')]}
#engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}?charset=utf8'.format(
if config['host'].startswith('/'):
    __fmt = 'mysql+mysqlconnector://{user}:{password}@/{database}?unix_socket={host}&charset=utf8'
else:
    __fmt = 'mysql+mysqlconnector://{user}:{password}@{host}/{database}?charset=utf8'
engine = create_engine(__fmt.format(
#engine = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8&use_unicode=0'.format(
#engine = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8&use_unicode=1'.format(
    **config
), echo=False, encoding='utf8')
