from contextlib import closing
import os
import pathlib
import pandas as pd
import sqlalchemy as sa
from prefect import task, flow, context
from prefect.deployments import Deployment
from prefect_shell import shell_run_command
from prefect.blocks.system import Secret
from prefect.server.schemas.schedules import CronSchedule


DATA_PATH = pathlib.Path(os.environ["DATAPATH"]) / "final"
CSV_FN = DATA_PATH / "can_scrape_api_covid_us.csv"
DATA_PATH.mkdir(parents=True, exist_ok=True)
FN_STR = "can_scrape_api_covid_us{}"

DATATYPES = {
    "provider": "category",
    "location_id": "category",
    "location": "int32",
    "location_type": "category",
    "variable_name": "category",
    "measurement": "category",
    "unit": "category",
    "age": "category",
    "race": "category",
    "sex": "category",
    "ethnicity": "category",
    "source_url": "object",
    "source_name": "object",
    "value": "float64",
}


@task(retries=3, retry_delay_seconds=30, timeout_seconds=60*60)
def export_to_csv(connstr: str):
    db = sa.create_engine(connstr)
    with open(CSV_FN, "w") as f:
        with closing(db.raw_connection()) as conn:
            with closing(conn.cursor()) as cur:
                cur.copy_expert(
                    "COPY (SELECT * From covid_us) TO STDOUT CSV HEADER;", f
                )

    return True


@task(retries=3, retry_delay_seconds=30, timeout_seconds=60*60)
def create_parquet():
    ts = context.get_run_context().start_time
    dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H%M%S")
    vintage_fn = FN_STR.format(dt_str) + ".parquet"
    fn = FN_STR.format("") + ".parquet"

    df = pd.read_csv(CSV_FN, parse_dates=["dt", "last_updated"], dtype=DATATYPES)
    df.to_parquet(DATA_PATH / vintage_fn, index=False)
    df.to_parquet(DATA_PATH / fn, index=False)
    return vintage_fn, fn


@task(timeout_seconds=60*60)
def get_gcs_cmd(fn):
    return f"gsutil acl ch -u AllUsers:R gs://can-scrape-outputs/final/{fn}"


@flow
def update_parquet_flow():
    connstr = Secret.load("covid-db-conn-uri").get()
    export_to_csv(connstr)
    vintage_fn, fn = create_parquet()
    shell_run_command(get_gcs_cmd(vintage_fn))
    shell_run_command(get_gcs_cmd(fn))


def deploy_update_parquet_flow():
    deployment: Deployment = Deployment.build_from_flow(
        update_parquet_flow,
        name="update-parquet-flow",
        # at 4 am every day
        schedule=CronSchedule(cron="0 4 * * *", timezone="America/New_York"),
    )
    deployment.apply()
