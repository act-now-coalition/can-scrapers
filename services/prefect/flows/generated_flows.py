from datetime import timedelta
from typing import Any, Tuple, Type

import pandas as pd
import sqlalchemy as sa
from can_tools import ALL_SCRAPERS
from can_tools.scrapers.base import DatasetBase

import prefect
from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret


@task
def create_scraper(cls: Type[DatasetBase], dt: pd.Timestamp) -> DatasetBase:
    logger = prefect.context.get("logger")
    logger.info("Creating class {} with dt = {}".format(cls, dt))
    return cls(execution_dt=dt)


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def fetch(d: DatasetBase):
    logger = prefect.context.get("logger")
    logger.info("About to run {}._fetch".format(d.__class__.__name__))
    logger.info("In fetch and have execution_dt = {}".format(d.execution_dt))
    fn = d._fetch()
    logger.info("{}._fetch success".format(d.__class__.__name__))
    logger.info("Saved raw data to: {}".format(fn))


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def normalize(d: DatasetBase):
    logger = prefect.context.get("logger")
    logger.info("About to run {}._normalize".format(d.__class__.__name__))
    logger.info("In _normalize and have execution_dt = {}".format(d.execution_dt))
    fn = d._normalize()
    logger.info("{}._normalize success".format(d.__class__.__name__))
    logger.info("Saved clean data to: {}".format(fn))


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def validate(d: DatasetBase):
    if not d._validate():
        raise ValueError("failed validation")


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def put(d: DatasetBase, connstr: str):
    logger = prefect.context.get("logger")

    engine = sa.create_engine(connstr)

    logger.info("About to run {}._put".format(d.__class__.__name__))
    logger.info("In _put and have execution_dt = {}".format(d.execution_dt))
    success, rows_in, rows_out = d._put(engine)

    logger.info("{}._put success".format(d.__class__.__name__))
    logger.info("Inserted {} rows".format(rows_in))
    logger.info("Deleted {} rows from temp table".format(rows_out))

    # do something with success
    return success


def create_flow_for_scraper(ix: int, d: Type[DatasetBase]):
    sched = CronSchedule(f"{ix % 60} */4 * * *")

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
