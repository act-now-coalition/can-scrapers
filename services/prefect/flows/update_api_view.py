from contextlib import closing
from datetime import timedelta

import pandas as pd
import prefect.context
import sqlalchemy as sa
from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret
from prefect.tasks.shell import ShellTask

DATA_PATH = pathlib.Path(os.environ["DATAPATH"]) / "final"
CSV_FN = DATA_PATH / "can_scrape_api_covid_us.csv"
DATA_PATH.mkdir(parents=True, exist_ok=True)
FN_STR = "can_scrape_api_covid_us{}"


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def export_to_csv(connstr: str):
    db = sa.create_engine(connstr)
    with open(CSV_FN, "w") as f:
        with closing(db.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.copy_expert(
                    "COPY (SELECT * From covid_us) TO STDOUT CSV HEADER;", f
                )

    return True


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def create_parquet():
    ts = prefect.context.scheduled_start_time
    dt_str = pd.to_datetime(ts).strftime("%Y-%m-%dT%H")
    vintage_fn = FN_STR.format(dt_str) + ".parquet"
    fn = FN_STR.format("") + ".parquet"

    df = pd.read_csv(CSV_FN, parse_dates=["dt"])
    df.to_parquet(DATA_PATH / vintage_fn, index=False)
    df.to_parquet(DATA_PATH / fn, index=False)
    return vintage_fn, fn


with Flow("Update parquet files", sched = CronSchedule("10 */2 * * *")) as f:
    connstr = EnvVarSecret("COVID_DB_CONN_URI")
    export_to_csv(connstr)
    vintage_fn, fn = create_parquet()
    ShellTask(command=f"gsutil acl ch -u AllUsers:R gs://final/{vintage_fn}")
    ShellTask(command=f"gsutil acl ch -u AllUsers:R gs://final/{fn}")

f.register(project_name="can-scrape")
