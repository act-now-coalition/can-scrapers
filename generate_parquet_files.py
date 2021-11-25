import os
import pandas as pd
import pathlib
import prefect
import sqlalchemy as sa

from prefect import Flow, task, unmapped
from prefect.tasks.secrets import EnvVarSecret

DATA_PATH = pathlib.Path(os.environ["DATAPATH"]) / "final"
DATA_PATH.mkdir(parents=True, exist_ok=True)
FILENAME_PREFIX = "can_scrape_api_covid_us"

@task
def fetch_location_ids(connstr: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        result = conn.execute(sa.text('SELECT DISTINCT location_id FROM data.covid_observations;'))
        location_ids = [row[0] for row in result.fetchall()]
        return location_ids

@task
def create_location_parquet(connstr: str, location_id: str):
    engine = sa.create_engine(connstr)
    with engine.connect() as conn:
        # Read rows from PostgreSQL into Pandas dataframe.
        query = sa.text('SELECT * FROM data.covid_observations WHERE location_id = :location_id').bindparams(location_id=location_id)
        df = pd.read_sql_query(query, conn)

        # Write vintage file.
        ts = prefect.context.scheduled_start_time
        dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H")
        vintage_fn = f'{FILENAME_PREFIX}_{location_id}_{dt_str}.parquet'
        df.to_parquet(DATA_PATH / vintage_fn, index=False)

        # Replace primary file.
        fn = f'{FILENAME_PREFIX}_{location_id}.parquet'
        df.to_parquet(DATA_PATH / fn, index=False)

def main():
    with Flow('update_location_parquet_files') as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        location_ids = fetch_location_ids(connstr)
        create_location_parquet.map(connstr=unmapped(connstr), location_id=location_ids)

    flow.run()

if __name__ == '__main__':
    main()
