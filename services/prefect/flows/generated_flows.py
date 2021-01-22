from datetime import timedelta
from typing import Any, Tuple, Type

import pandas as pd
import sqlalchemy as sa
from can_tools import ALL_SCRAPERS
from can_tools.scrapers.base import DatasetBase

from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret
from prefect.engine.results import GCSResult


@task
def create_scraper(cls: Type[DatasetBase], dt: pd.Timestamp) -> DatasetBase:
    return cls(execution_dt=dt)


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def fetch(d: DatasetBase) -> Tuple[Any, DatasetBase]:
    return d.fetch()


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def normalize(d: DatasetBase, data: Any) -> pd.DataFrame:
    return d.normalize(data)


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def validate(d: DatasetBase, df: pd.DataFrame) -> bool:
    if d.validate(df, None):
        return True
    else:
        raise ValueError("failed validation")


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def put(d: DatasetBase, df: pd.DataFrame, connstr: str) -> bool:
    engine = sa.create_engine(connstr)
    success = d.put(engine, df)

    # do something with success
    return success


def create_flow_for_scraper(ix:int, d: Type[DatasetBase]):
    sched = CronSchedule(f"{ix} */4 * * *")
    result = GCSResult(bucket="can-scrape-outputs")

    with Flow(cls.__name__, sched, result=result) as flow:
        ts = pd.Timestamp.utcnow()
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        d = create_scraper(cls, ts)
        data = fetch(d)
        df = normalize(d, data)
        validate(d, df)
        put(d, df, connstr)

    return flow


for ix, cls in enumerate(ALL_SCRAPERS):
    if not cls.autodag:
        continue
    flow = create_flow_for_scraper(ix, cls)
    flow.register(project_name="can-scrape")
