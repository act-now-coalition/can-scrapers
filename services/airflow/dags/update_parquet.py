import io
import os
import pathlib

import pandas as pd

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from common import default_dag_kw

DATA_PATH = pathlib.Path(os.environ["DATAPATH"]) / "final"
CSV_FN = DATA_PATH / "can_scrape_api_covid_us.csv"
DATA_PATH.mkdir(parents=True, exist_ok=True)

FN_STR = "can_scrape_api_covid_us{}.parquet"


def export_to_csv(ts, **kw):
    db = PostgresHook(postgres_conn_id="postgres_covid")
    db.bulk.dump("api.covid_us", CSV_FN)


def create_parquet(ts, **kw):
    dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H")
    print("dt and ts:", dt_str, ts)

    vintage_fn = DATA_PATH / FN_STR.format(dt_str)
    fn = DATA_PATH / FN_STR.format("")

    df = pd.read_csv(CSV_FN, parse_dates=["dt"])
    df.to_parquet(vintage_fn, index=False)
    df.to_parquet(fn, index=False)


with DAG(
    **default_dag_kw("update_covid_us_api", schedule_interval="20 */4 * * *")
) as dag:

    csv_export = PythonOperator(
        python_callable=export_to_csv, task_id="csv_export", dag=dag
    )
    to_parquet = PythonOperator(
        python_callable=create_parquet,
        provide_context=True,
        task_id="to_parquet",
        dag=dag,
    )

    update_table = PostgresOperator(
        task_id="update_covid_us_matview",
        sql="REFRESH MATERIALIZED VIEW api.covid_us;",
        dag=dag,
    )

    public_first_vintage_fn = BashOperator(
        task_id="vintage_file_public",
        bash_command="gsutil acl ch -u AllUsers:R gs://us-east4-data-eng-scrapers-a02dc940-bucket/data/final/$(FN_BASE){{ execution_date.strftime('%Y-%m-%dT%H') }}.parquet",
        env={"FN_BASE": FN_STR.format("")},
        dag=dag,
    )

    public_latest_file = BashOperator(
        task_id="latest_file_public",
        bash_command="gsutil acl ch -u AllUsers:R gs://us-east4-data-eng-scrapers-a02dc940-bucket/data/final/$(FN_BASE).parquet",
        env={"FN_BASE": FN_STR.format("")},
        dag=dag,
    )

    update_table >> csv_export >> to_parquet >> [
        public_first_vintage_fn,
        public_latest_file,
    ]
