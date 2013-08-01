import nuus
from sqlalchemy import create_engine

config = {x[9:].lower(): nuus.app.config[x] for x in filter(lambda x: x.startswith('DATABASE_'), nuus.app.config.keys())}
engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}/{database}'.format(
    **config
), echo=True)
