# TODO: The prefect and pyarrow packages are not part of either Pipfile or
# requirements.txt. They're currently manually installed in production via
# services/prefect/setup_gcp_instance.sh. We should probably fix that.

# 1. is anything bad gonna happen when we scale to 3k or 30k?
# 2. how is this parallelized? do we scale out more instances?
# - Sean says there's a bucket with the intermediate data of every scraper
# - is the data being passed directly or being spit out to S3?

"""
Use [technology X] to build a minimal pipeline that:

1. Pulls in:
    1. Our scraped data from https://storage.googleapis.com/can-scrape-outputs/final/can_scrape_api_covid_us.parquet (430MB)
    2. Location data from https://media.githubusercontent.com/media/covid-projections/covid-data-model/main/data/geo-data.csv
    3. Population data from https://media.githubusercontent.com/media/covid-projections/covid-data-public/main/data/misc/fips_population.csv
2. Does some minimal processing, e.g.:
    1. Uses USA Facts case data everywhere except Nebraska counties where we’ll arbitrarily use the CDC2 case data.
    2. Calculates daily new cases from cumulative cases.
    3. [bonus, maybe worth doing if we’re experimenting with something not pandas-based] port our outlier detection code over to remove outliers in new cases.
    4. Calculates a 7-day average of new cases.
    5. Calculates 7-day average of new cases per 100k population.
3. Generates a resulting dataset that contains 7-day average per 100k population for every location.
4. Lets you (somehow, hacky is fine) block data for a single location, ideally without having to re-run the entire pipeline for all locations (or just prove it’ll be really fast even with a more complex pipeline).

The goal would be to write up an evaluation of the experience against the above requirements and limitations of our current pipeline to see how good of a fit the technology might be.

---

Spin up a prefect environment (good learning in itself).
Write a task that reads in all of our data which is probably just import pandas as pd; dataframe = pd.read_parquet('https://storage.googleapis.com/can-scrape-outputs/final/can_scrape_api_covid_us.parquet')
At that point I’d do some pandas tutorials to get a sense for how pandas works. :slightly_smiling_face:
Then write some tasks that do some minimal transformation on the data.  E.g.
Filter it to the data with variable=cases, provider=usafacts, age=all, race=all, sex=all, gender=all  which should give you cumulative cases for every location.
From the cumulatives, subtract the day-over-day values to generate “new cases” for each day.
And just play around with running that “pipeline” and how prefect works.  We can come up with next steps from there.
"""

import argparse
import pandas as pd

from datetime import date, timedelta
from prefect import Flow, Parameter, task, unmapped
from prefect.engine import signals
from typing import List

COVID_DATA_PATH_PREFIX = "./tmp/final/can_scrape_api_covid_us"
GEO_DATA_PATH = "https://media.githubusercontent.com/media/covid-projections/covid-data-model/main/data/geo-data.csv"


@task
def location_ids_for(state: str, geo_data_path: str = GEO_DATA_PATH) -> List[str]:
    df = pd.read_csv(geo_data_path)
    df = df[df["state"] == state]
    return df['location_id'].tolist()

@task
def daily_new_cases_for(location_id: str, sources: List[str], smooth: int) -> float:
    path = f"{COVID_DATA_PATH_PREFIX}_{location_id}.parquet"

    try:
        df = pd.read_parquet(path)
    except FileNotFoundError:
        # TODO: report the error somewhere. Sentry?
        raise signals.SKIP()

    # Filter to the given list of sources.
    if len(sources) > 0:
        # TODO: back off to other sources
        source = sources[0]
        df = df[df["source_name"] == source]

    # Filter to the most recent dates.
    # TODO: replace max date with date.today()
    # TODO: what to do if not enough dates to satisfy smoothing?
    # max_date = date.today()
    max_date = df["dt"].max()
    dates = [max_date - timedelta(days=days) for days in range(0, smooth + 1)]
    df = df[df["dt"].isin(dates)]

    df = df[df["variable_name"] == "cases"]

    # TODO: maybe better to do this in pandas? or reuse existing code in
    # covid-data-model repo. i'm just pulling it out into plain Python data
    # structures since i don't know pandas / existing code well.
    cumulative_case_records = df[["dt", "value"]].sort_values("dt").values.tolist()
    new_case_records = [
        (date2, cases2 - cases1)
        for (date1, cases1), (date2, cases2) in zip(
            cumulative_case_records, cumulative_case_records[1:]
        )
    ]
    avg_daily_new_cases = sum(new_cases for d, new_cases in new_case_records) / smooth

    print(f"{location_id}: {avg_daily_new_cases} new cases ({smooth}-day average)")

    return avg_daily_new_cases


@task
def log_data(df):
    print(df)

def create_flow():
    with Flow("ProcessLocations") as flow:
        geo_data_path = Parameter(
            "geo_data_path",
            default=GEO_DATA_PATH
        )
        state = Parameter("state", default=[])
        sources = Parameter("sources", default=["USAFacts"])
        smooth = Parameter("smooth", default=7)

        location_ids = location_ids_for(state, geo_data_path)
        daily_new_cases = daily_new_cases_for.map(
            location_ids, unmapped(sources), unmapped(smooth)
        )

    return flow


def main():
    parser = argparse.ArgumentParser(
        description="Generate basic COVID metrics for the given location(s)"
    )
    parser.add_argument(
        "--state",
        "--states",
        help="comma-separated list of two-letter state abbreviation(s)",
        default='USAFacts',
    )
    parser.add_argument(
        "--source",
        "--sources",
        help="comma-separated list of source(s) to use (e.g. USAFacts, Centers for Disease Control and Prevention)",
    )
    args = parser.parse_args()

    sources = []
    if args.source:
        sources = [source.strip() for source in args.source.split(",")]

    states = []
    if args.state:
        states = [state.strip() for state in args.state.split(",")]

    for state in states:
        flow = create_flow()
        flow.run(
            geo_data_path="./geo-data.csv",
            state=state,
            sources=sources,
        )


if __name__ == "__main__":
    main()
