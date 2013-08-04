import nuus
from sqlalchemy import create_engine

config = {x[9:].lower(): nuus.app.config[x] for x in [x for x in list(nuus.app.config.keys()) if x.startswith('DATABASE_')]}
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}?charset=utf8'.format(
#engine = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8&use_unicode=0'.format(
#engine = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8&use_unicode=1'.format(
    **config
), echo=False, encoding='utf8')
