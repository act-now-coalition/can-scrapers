import os
import pathlib
from contextlib import closing

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from common import default_dag_kw

DATA_PATH = pathlib.Path(os.environ["DATAPATH"]) / "final"
CSV_FN = DATA_PATH / "can_scrape_api_covid_us.csv"
DATA_PATH.mkdir(parents=True, exist_ok=True)

FN_STR = "can_scrape_api_covid_us{}"


def export_to_csv():
    db = PostgresHook(postgres_conn_id="postgres_covid")
    with open(CSV_FN, "w") as f:
        with closing(db.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.copy_expert(
                    "COPY (SELECT * From covid_us) TO STDOUT CSV HEADER;", f
                )


def create_parquet(ts, **kw):
    dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H")
    print("dt and ts:", dt_str, ts)

    vintage_fn = DATA_PATH / (FN_STR.format(dt_str) + ".parquet")
    fn = DATA_PATH / (FN_STR.format("") + ".parquet")

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

    public_first_vintage_fn = BashOperator(
        task_id="vintage_file_public",
        bash_command="gsutil acl ch -u AllUsers:R gs://us-east4-data-eng-scrapers-a02dc940-bucket/data/final/{{ params.FN_BASE }}{{ execution_date.strftime('%Y-%m-%dT%H') }}.parquet",
        params={"FN_BASE": FN_STR.format("")},
        dag=dag,
    )

    public_latest_file = BashOperator(
        task_id="latest_file_public",
        bash_command="gsutil acl ch -u AllUsers:R gs://us-east4-data-eng-scrapers-a02dc940-bucket/data/final/{{ params.FN_BASE }}.parquet",
        params={"FN_BASE": FN_STR.format("")},
        dag=dag,
    )

    csv_export >> to_parquet >> [
        public_first_vintage_fn,
        public_latest_file,
    ]
