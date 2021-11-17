import os
import sqlalchemy.orm as sa_orm
import sqlalchemy as sa
from can_tools.models import bootstrap

db_url = os.environ.get('CAN_SCRAPERS_DB_URL')
if not db_url:
    raise RuntimeError('Missing CAN_SCRAPERS_DB_URL environment variable.')

engine = sa.create_engine(db_url, echo=False)
Session = sa_orm.sessionmaker(bind=engine)
sess = Session(bind=engine)

bootstrap(sess, delete_first=False)
