import logging
import sqlalchemy as sa
import sys

from contextlib import closing
from prefect import Flow, Parameter, task, unmapped
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
        return location_ids

@task
def process_location(connstr: str, location_id: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        rows = conn.execute(sa.text('SELECT * FROM data.covid_observations WHERE location_id = :location_id'), {'location_id': location_id}).fetchall()
    print(rows)

def main():
    with Flow('update_parquet_files') as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        location_ids = fetch_location_ids(connstr)
        process_location.map(connstr=unmapped(connstr), location_id=location_ids)

    flow.run()

if __name__ == '__main__':
    main()
