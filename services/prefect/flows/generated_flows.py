from typing import Any, Type, List
import sqlalchemy as sa

from can_tools import ACTIVE_SCRAPERS
import can_tools
from can_tools.scrapers.base import DatasetBase
from prefect import flow, task, get_run_logger, context, runtime
from prefect.blocks.system import Secret

from can_tools.scrapers.official.base import CacheMixin
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule


@task(timeout_seconds=3*60*60)
def create_scraper(cls: Type[DatasetBase], **kwargs) -> DatasetBase:
    logger = get_run_logger()
    dt = context.get_run_context().start_time
    logger.info("Creating class {} with dt = {}".format(cls, dt))
    return cls(execution_dt=dt, **kwargs)


@task(retries=3, retry_delay_seconds=30, timeout_seconds=3*60*60)
def fetch(d: DatasetBase):
    logger = get_run_logger()
    logger.info("About to run {}._fetch".format(d.__class__.__name__))
    logger.info("In fetch and have execution_dt = {}".format(d.execution_dt))
    fn = d._fetch()
    logger.info("{}._fetch success".format(d.__class__.__name__))
    logger.info("Saved raw data to: {}".format(fn))


@task(timeout_seconds=3*60*60)
def normalize(d: DatasetBase):
    logger = get_run_logger()
    logger.info("About to run {}._normalize".format(d.__class__.__name__))
    logger.info("In _normalize and have execution_dt = {}".format(d.execution_dt))
    fn = d._normalize()
    logger.info("{}._normalize success".format(d.__class__.__name__))
    logger.info("Saved clean data to: {}".format(fn))


@task(retries=3, retry_delay_seconds=30, timeout_seconds=3*60*60)
def put(d: DatasetBase, engine: sa.engine.Engine):
    logger = get_run_logger()
    logger.info("About to run {}._put".format(d.__class__.__name__))
    logger.info("In _put and have execution_dt = {}".format(d.execution_dt))
    success, rows_in, rows_out = d._put(engine)

    logger.info("{}._put success".format(d.__class__.__name__))
    logger.info("Inserted {} rows".format(rows_in))
    logger.info("Deleted {} rows from temp table".format(rows_out))

    # do something with success
    return success


@task(timeout_seconds=3*60*60)
def check_if_new_data_for_flow(cls):
    if issubclass(cls, CacheMixin):
        return cls().check_if_new_data_and_update()
    return True  # if class does not have etag checking always execute the scraper


@task(timeout_seconds=3*60*60)
def get_scraper_from_classname(cls_name: str) -> Type[DatasetBase]:
    cls = getattr(can_tools.scrapers, cls_name)
    if cls is None:
        raise ValueError(f"Could not find scraper class {cls_name}")
    return cls


@flow(flow_run_name="{cls_name}")
def create_scraper_flow(cls_name: str) -> None:
    cls = get_scraper_from_classname(cls_name)

    if check_if_new_data_for_flow(cls):
        connstr = Secret.load("covid-db-conn-uri").get()
        engine = sa.create_engine(connstr)

        scraper = create_scraper(cls)
        fetch(scraper)
        normalize(scraper)
        put(scraper, engine)
    else:
        logger = get_run_logger()
        logger.info(
            f"Skipping {cls_name} because no new data was found since last run."
        )


@flow
def create_main_flow() -> None:
    """Main flow for orchestrating all scraper runs."""
    for scraper in ACTIVE_SCRAPERS:
        try:
            create_scraper_flow(scraper.__name__)
        except Exception as e:
            logger = get_run_logger()
            logger.error(
                f"Failed to create scraper flow for {scraper.__name__} with error {e}"
            )


def build_scraper_flow_deployments() -> List[Deployment]:
    deployments = []
    for scraper in ACTIVE_SCRAPERS:
        deployments.append(
            Deployment.build_from_flow(
                create_scraper_flow,
                name=scraper.__name__,
                parameters=dict(cls_name=scraper.__name__),
            )
        )
    return deployments


def deploy_scraper_flows():
    for deployment in build_scraper_flow_deployments():
        deployment.apply()

    main_flow_deployment: Deployment = Deployment.build_from_flow(
        create_main_flow,
        name="main_flow",
        # cron schedule to run every 5 hours
        schedule=CronSchedule(cron="0 */5 * * *", timezone="America/New_York"),
    )
    main_flow_deployment.apply()
