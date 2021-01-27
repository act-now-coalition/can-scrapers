from datetime import timedelta
from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret
import sqlalchemy as sa


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def truncate_table(connstr: str, table_name: str):
    engine = sa.create_engine(connstr)
    sql = f"truncate table {table_name}"
    engine.execute(sql)
    return True


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def reset_sequence(connstr: str, seq_name: str, _ready: bool):
    engine = sa.create_engine(connstr)
    sql = f"alter sequence {seq_name} restart"
    engine.execute(sql)


def create_flow_for_table(table_name):
    sched = CronSchedule("50 */2 * * *")
    tn = f"data.{table_name}"
    sn = f"{tn}_id_seq"
    with Flow(f"clean-sql-{table_name}", sched) as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        ready = truncate_table(connstr, tn)
        reset_sequence(connstr, sn, ready)

    return flow


for tn in ["temp_official_no_location", "temp_official_has_location"]:
    flow = create_flow_for_table(tn)
    flow.register(project_name="can-scrape")
