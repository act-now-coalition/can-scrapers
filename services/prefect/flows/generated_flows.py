from typing import Any, Type, List
from datetime import timedelta
import sentry_sdk
import sqlalchemy as sa

from can_tools import ALL_SCRAPERS, scrapers
from can_tools.scrapers.base import DatasetBase
import prefect
from prefect import Flow, task, case
from prefect.engine import signals
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret
from prefect.tasks.prefect.flow_run import StartFlowRun
from can_tools.scrapers.official.base import ETagCacheMixin
from services.prefect.flows.utils import (
    etag_caching_terminal_state_handler, skip_if_running_handler
)

# A list of flows that we want to be registered, but not included in the MainFlow runs. 
FLOWS_EXCLUDED_FROM_MAIN_FLOW: List[Flow] = [scrapers.NYTimesCasesDeaths]


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


@task
def skip_cached_flow():
    # Set the task state to skipped. The flow's terminal state
    # handler (etag_caching_terminal_state_handler) will see the etag_skip_flag
    # context item and will in turn set the final state of the flow to skipped.
    message = "No new source data. Skipping..."
    skip_signal = signals.SKIP(message=message, context=dict(etag_skip_flag=True))
    raise skip_signal


@task
def check_if_new_data_for_flow(cls):
    if issubclass(cls, ETagCacheMixin):
        return cls().check_if_new_data()
    return True  # if class does not have etag checking always execute the scraper


def create_flow_for_scraper(ix: int, cls: Type[DatasetBase], schedule=True):
    sched = None
    if schedule:
        sched = CronSchedule(f"{ix % 60} */4 * * *")

    with Flow(
        cls.__name__, sched, 
        state_handlers=[skip_if_running_handler],
        terminal_state_handler=etag_caching_terminal_state_handler
    ) as flow:

        # check if scraper has etag checking
        # if so and the data has been updated since the last check set new_data to True
        new_data = check_if_new_data_for_flow(cls)

        with case(new_data, True):
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

        with case(new_data, False):
            skip_cached_flow()

    return flow


def create_main_flow(flows: List[Flow], project_name):
    schedule = CronSchedule("0 */4 * * *")

    with Flow("MainFlow", schedule) as main_flow:
        tasks = []
        for flow in flows:
            task = StartFlowRun(
                flow_name=flow.name,
                project_name=project_name,
                wait=True,
                timeout=7200,  # timeout flows after 2 hours
            )
            tasks.append(task)

        parquet_flow = StartFlowRun(
            flow_name="UpdateParquetFiles",
            project_name=project_name,
            wait=True,
            timeout=3600,  # timeout parquet flow after 1 hour
        )
        # Always run parquet flow
        parquet_flow.trigger = prefect.triggers.all_finished

        for task in tasks:
            task.set_downstream(parquet_flow)

    return main_flow


def init_flows():
    flows = []
    for ix, cls in enumerate(ALL_SCRAPERS):
        if not cls.autodag:
            continue
        flow = create_flow_for_scraper(ix, cls, schedule=False)
        flows.append(flow)
        flow.register(project_name="can-scrape")
        
    flows = [flow for flow in flows if flow not in FLOWS_EXCLUDED_FROM_MAIN_FLOW]
    flow = create_main_flow(flows, "can-scrape")
    flow.register(project_name="can-scrape")


if __name__ == "__main__":
    init_flows()
