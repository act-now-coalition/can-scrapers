import logging
import sqlalchemy as sa
import sys

from prefect import task, Flow, Parameter
from prefect.tasks.secrets import EnvVarSecret

#logging.basicConfig(
#        stream=sys.stdout,
#        level=logging.INFO)
        #format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s')
        #format='[%(asctime)s] %(levelname)s - %(message)s')

@task
def fetch_location_ids(connstr: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        result = conn.execute(sa.text('SELECT DISTINCT location_id FROM data.covid_observations;'))
        location_ids = [row[0] for row in result.fetchall()]
        print('foo')
        return location_ids

@task
def process_location(location_id: str):
    print(location_id)

def main():
    with Flow('update_parquet_files') as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        location_ids = fetch_location_ids(connstr)
        process_location.map(location_ids)

    flow.run()

if __name__ == '__main__':
    main()
