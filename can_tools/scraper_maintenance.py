"""
Utility functions for performing scraper maintenance such as
re-running scraper functions for multiple vintages.
"""

import datetime
import logging
from typing import Optional, Type

import click
import pandas as pd
import sqlalchemy
from sqlalchemy.engine.base import Engine

from can_tools import ALL_SCRAPERS
from can_tools.scrapers import base

_logger = logging.getLogger(__name__)


@click.group()
def main():
    pass


def _choose_scraper(scraper_name) -> Type[base.DatasetBase]:
    matching_scrapers = [cls for cls in ALL_SCRAPERS if cls.__name__ == scraper_name]

    # Must match because scraper_name must be a choice of all existing scrapers.
    return matching_scrapers[-1]


@main.command()
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

    scraper_cls = _choose_scraper(scraper_name)
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


@main.command()
@click.argument(
    "scraper_name", type=click.Choice(sorted([cls.__name__ for cls in ALL_SCRAPERS]))
)
@click.option("--fips", "-f", type=int)
@click.option("--location-type", "-l", type=click.Choice(["state", "county"]))
@click.option("--days-back", "-d", type=int, default=7)
def latest_vaccine_data(
    scraper_name: str, fips: Optional[str], location_type: Optional[str], days_back: int
):
    """Prints out latest vaccination data for a given scraper"""
    scraper_cls = _choose_scraper(scraper_name)
    scraper = scraper_cls()
    normalized_data = scraper.fetch_normalize()
    vaccine_variables = ["total_vaccine_initiated", "total_vaccine_completed"]
    start_date = pd.Timestamp(
        normalized_data.dt.max() - datetime.timedelta(days=days_back)
    )
    params = [
        "category == @vaccine_variables ",
        "race == 'all'",
        "age == 'all'",
        "ethnicity == 'all'",
        "sex == 'all'",
        "dt > @start_date",
    ]
    if fips:
        params.append("location == @fips")
    if location_type:
        params.append("location_type == @location_type")

    query_string = " & ".join(params)

    subset = normalized_data.query(query_string)
    columns = ["dt", "category", "value"]
    index = ["dt"]

    pd.options.display.max_rows = 1000
    pd.options.display.max_columns = 100

    # Only adding location or location name if they actually exist.
    if "location" in subset.columns:
        columns.append("location")
        index.append("location")
    if "location_name" in subset.columns:
        columns.append("location_name")
        index.append("location_name")

    index.append("category")

    subset = subset.loc[:, columns].set_index(index).unstack(level=-1)
    print(subset)


if __name__ == "__main__":
    main()
