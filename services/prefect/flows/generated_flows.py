from typing import Any, Tuple, Type, List
from datetime import timedelta
import sentry_sdk
import pandas as pd
import sqlalchemy as sa
from can_tools import ALL_SCRAPERS
from can_tools.scrapers.base import DatasetBase
from can_tools.scrapers.base import ALL_STATES_PLUS_DC
from can_tools.scrapers import CDCCovidDataTracker
import prefect
from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret
from prefect.tasks.prefect.flow_run import StartFlowRun


@task
def create_scraper(cls: Type[DatasetBase], **kwargs) -> DatasetBase:
    logger = prefect.context.get("logger")
    dt = prefect.context.get("scheduled_start_time")
    logger.info("Creating class {} with dt = {}".format(cls, dt))
    return cls(execution_dt=dt, **kwargs)


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def fetch(d: DatasetBase):
    logger = prefect.context.get("logger")
    logger.info("About to run {}._fetch".format(d.__class__.__name__))
    logger.info("In fetch and have execution_dt = {}".format(d.execution_dt))
    fn = d._fetch()
    logger.info("{}._fetch success".format(d.__class__.__name__))
    logger.info("Saved raw data to: {}".format(fn))


@task()
def normalize(d: DatasetBase):
    logger = prefect.context.get("logger")
    logger.info("About to run {}._normalize".format(d.__class__.__name__))
    logger.info("In _normalize and have execution_dt = {}".format(d.execution_dt))
    fn = d._normalize()
    logger.info("{}._normalize success".format(d.__class__.__name__))
    logger.info("Saved clean data to: {}".format(fn))


@task()
def validate(d: DatasetBase):
    # validation either completes successfully or raises an exception
    d._validate()


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


@task()
def initialize_sentry(sentry_dsn: str):
    """Initialize sentry SDK for Flow. """
    sentry_sdk.init(sentry_dsn)
    sentry_sdk.set_tag("flow", prefect.context.flow_name)


def create_flow_for_scraper(ix: int, cls: Type[DatasetBase], schedule=True):
    sched = None
    if schedule:
        sched = CronSchedule(f"{ix % 60} */4 * * *")

    with Flow(cls.__name__, sched) as flow:
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        sentry_dsn = EnvVarSecret("SENTRY_DSN")
        sentry_sdk_task = initialize_sentry(sentry_dsn)

        d = create_scraper(cls)
        fetched = fetch(d)
        normalized = normalize(d)
        validated = validate(d)
        done = put(d, connstr)

        d.set_upstream(sentry_sdk_task)
        normalized.set_upstream(fetched)
        validated.set_upstream(normalized)
        done.set_upstream(validated)

    return flow


def create_cdc_single_state_flow():
    with Flow(CDCCovidDataTracker.__name__) as flow:
        state = prefect.Parameter("state")
        connstr = EnvVarSecret("COVID_DB_CONN_URI")
        sentry_dsn = EnvVarSecret("SENTRY_DSN")
        sentry_sdk_task = initialize_sentry(sentry_dsn)

        d = create_scraper(CDCCovidDataTracker, state=state)
        fetched = fetch(d)
        normalized = normalize(d)
        validated = validate(d)
        done = put(d, connstr)

        d.set_upstream(sentry_sdk_task)
        normalized.set_upstream(fetched)
        validated.set_upstream(normalized)
        done.set_upstream(validated)

    return flow


def create_cdc_all_states_flow(schedule=True):
    """Creates a flow that runs the CDC data update on all states."""
    sched = None
    if schedule:
        sched = CronSchedule("17 */4 * * *")

    flow = Flow("CDCAllStatesDataUpdate", sched)
    for state in ALL_STATES_PLUS_DC:
        task = StartFlowRun(
            flow_name=CDCCovidDataTracker.__name__,
            project_name="can-scrape",
            wait=True,
            parameters={"state": state.abbr},
        )
        flow.add_task(task)

    return flow


def create_main_flow(flows: List[Flow], project_name):
    schedule = CronSchedule("0 */3 * * *")

    with Flow("MainFlow", schedule) as main_flow:
        tasks = []
        for flow in flows:
            task = StartFlowRun(
                flow_name=flow.name, project_name=project_name, wait=True
            )
            tasks.append(task)

        parquet_flow = StartFlowRun(
            flow_name="UpdateParquetFiles", project_name=project_name, wait=True
        )

        for task in tasks:
            task.set_downstream(parquet_flow)

    return main_flow


def init_flows():
    flows = []
    for ix, cls in enumerate(ALL_SCRAPERS):
        if not cls.autodag:
            continue
        if cls == CDCCovidDataTracker:
            flow = create_cdc_single_state_flow()
        else:
            flow = create_flow_for_scraper(ix, cls, schedule=False)
            flows.append(flow)
        flow.register(project_name="can-scrape")

    # Create additional flow that runs the CDC Data updater
    flow = create_cdc_all_states_flow(schedule=False)
    flows.append(flow)
    flow.register(project_name="can-scrape")

    flow = create_main_flow(flows, "can-scrape")
    flow.register(project_name="can-scrape")


if __name__ == "__main__":
    init_flows()
