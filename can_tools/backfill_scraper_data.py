from typing import Type, Optional

import logging

from sqlalchemy.engine.base import Engine
import sqlalchemy
import pandas as pd
import click

from can_tools.scrapers import base
from can_tools import ALL_SCRAPERS


_logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    "scraper_name", type=click.Choice(sorted([cls.__name__ for cls in ALL_SCRAPERS]))
)
@click.option("--start", "-s", type=pd.Timestamp)
@click.option("--end", "-s", type=pd.Timestamp)
@click.option("--latest", is_flag=True)
@click.option("--continue-on-fail", is_flag=True)
@click.option("--db-connection-str", envvar="COVID_DB_CONN_URI", required=True)
def rerun_scraper(
    scraper_name: str,
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
    latest: bool,
    continue_on_fail: bool,
    db_connection_str,
):

    matching_scrapers = [cls for cls in ALL_SCRAPERS if cls.__name__ == scraper_name]

    # Must match because scraper_name must be a choice of all existing scrapers.
    scraper_cls = matching_scrapers[-1]

    times = scraper_cls.find_previous_fetch_execution_dates(
        start_date=start, end_date=end, only_last=latest
    )
    click.echo(f"Re-running {scraper_name} on the following dates")
    times_formatted = "\n ".join([str(time) for time in times])
    click.echo(f"{times_formatted}")
    click.confirm("Continue?", abort=True)

    engine = sqlalchemy.create_engine(db_connection_str)

    for time in times:
        click.echo(f"Running {scraper_name} for time {time}")
        scraper = scraper_cls(time)
        try:
            scraper.reprocess_from_already_fetched_data(engine)
        except Exception:
            if continue_on_fail:
                _logger.exception("Scraper failed, continuing...")
                continue
            else:
                raise


if __name__ == "__main__":
    rerun_scraper()
