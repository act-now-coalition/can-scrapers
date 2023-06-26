from typing import List
from prefect import flow, task
import sqlalchemy as sa
from prefect.blocks.system import Secret

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule


@task
def truncate_table(connstr: str, table_name: str):
    engine = sa.create_engine(connstr)
    sql = f"truncate table {table_name}"
    engine.execute(sql)
    return True


@task
def reset_sequence(connstr: str, seq_name: str, _ready: bool):
    engine = sa.create_engine(connstr)
    sql = f"alter sequence {seq_name} restart"
    engine.execute(sql)


@flow
def create_flow_for_table(table_name):
    # sched = CronSchedule("50 */2 * * *")
    tn = f"data.{table_name}"
    sn = f"{tn}_id_seq"
    connstr = Secret.load("covid-db-conn-uri").get()
    ready = truncate_table(connstr, tn)
    reset_sequence(connstr, sn, ready)


def build_table_flow_deployments(table_names: List[str]) -> List[Deployment]:
    deployments: List[Deployment] = []
    for table_name in table_names:
        deployments.append(
            Deployment.build_from_flow(
                create_flow_for_table,
                name=f"clean-sql-{table_name}",
                parameters=dict(table_name=table_name),
                # At 50 minutes past the hour, every 4 hours
                schedule=CronSchedule(cron="50 */4 * * *", timezone="America/New_York"),
            )
        )
    return deployments


def deploy_clean_sql_flows():
    deployments: List[Deployment] = build_table_flow_deployments(
        ["temp_official_no_location", "temp_official_has_location"]
    )
    for deployment in deployments:
        deployment.apply()
