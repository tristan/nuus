import nuus
from sqlalchemy import create_engine

config = {x[9:].lower(): nuus.app.config[x] for x in filter(lambda x: x.startswith('DATABASE_'), nuus.app.config.keys())}
#engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}?charset=utf8'.format(
engine = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8&use_unicode=0'.format(
#engine = create_engine('mysql+mysqldb://{user}:{password}@{host}/{database}?charset=utf8&use_unicode=1'.format(
    **config
), echo=False, encoding='utf8')
