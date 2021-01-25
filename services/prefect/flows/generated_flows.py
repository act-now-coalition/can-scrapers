from datetime import timedelta
from typing import Any, Tuple, Type

import pandas as pd
import sqlalchemy as sa
from can_tools import ALL_SCRAPERS
from can_tools.scrapers.base import DatasetBase

from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret


@task
def create_scraper(cls: Type[DatasetBase], dt: pd.Timestamp) -> DatasetBase:
    return cls(execution_dt=dt)


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def fetch(d: DatasetBase):
    d._fetch()


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def normalize(d: DatasetBase):
    d._normalize()


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def validate(d: DatasetBase):
    if not d._validate():
        raise ValueError("failed validation")


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def put(d: DatasetBase, connstr: str):
    engine = sa.create_engine(connstr)
    success = d._put(engine)

    # do something with success
    return success


def create_flow_for_scraper(ix:int, d: Type[DatasetBase]):
    sched = CronSchedule(f"{ix} */4 * * *")

    with Flow(cls.__name__, sched) as flow:
        ts = pd.Timestamp.utcnow()
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        d = create_scraper(cls, ts)
        fetched = fetch(d)
        normalized = normalize(d)
        validated = validate(d)
        done = put(d, connstr)

        normalized.set_upstream(fetched)
        validated.set_upstream(normalized)
        done.set_upstream(validated)



    return flow


for ix, cls in enumerate(ALL_SCRAPERS):
    if not cls.autodag:
        continue
    flow = create_flow_for_scraper(ix, cls)
    flow.register(project_name="can-scrape")
