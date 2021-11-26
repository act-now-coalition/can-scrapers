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

from prefect import Flow, Parameter, task, unmapped
from prefect.engine import signals
from typing import List

COVID_DATA_PATH_PREFIX = "./tmp/final/can_scrape_api_covid_us"
GEO_DATA_PATH = "https://media.githubusercontent.com/media/covid-projections/covid-data-model/main/data/geo-data.csv"


@task
def location_ids_for(states: List[str], geo_data_path: str = GEO_DATA_PATH) -> List[str]:
    df = pd.read_csv(geo_data_path)
    if len(states) > 0:
        df = df[df["state"].isin(states)]

    return df['location_id'].tolist()

@task
def fetch_parquet_data(location_id: str, sources: List[str]) -> pd.DataFrame:
    path = f'{COVID_DATA_PATH_PREFIX}_{location_id}.parquet'

    try:
        df = pd.read_parquet(path)
        if len(sources) > 0:
            df = df[df['source_name'].isin(sources)]
    except FileNotFoundError:
        # TODO: report the error somewhere. Sentry?
        raise signals.SKIP()

    return df

@task
def process_dataframe(df: pd.DataFrame):
    cases_df = df[df["variable_name"] == "cases"]


@task
def fetch_csv_data(path: str):
    return pd.read_csv(path)


@task
def log_data(df):
    print(df)

def create_flow():
    with Flow("ProcessLocations") as flow:
        geo_data_path = Parameter(
            "geo_data_path",
            default=GEO_DATA_PATH
        )
        states = Parameter("states", default=[])
        sources = Parameter("sources", default=[])

        location_ids = location_ids_for(states, geo_data_path)
        dataframes = fetch_parquet_data.map(location_ids, unmapped(sources))

        log_data.map(dataframes)

        # covid_data_path = Parameter("covid_data_path", default="")

        # covid_data = fetch_parquet_data(covid_data_path)
        # geo_data = fetch_csv_data(geo_data_path)

        # log_data(covid_data)
        # log_data(geo_data)

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

    flow = create_flow()
    flow.run(
        #covid_data_path="./tmp/final/can_scrape_api_covid_us_iso1:us#iso2:us-vi#fips:78010.parquet",
        geo_data_path="./geo-data.csv",
        states=states,
        sources=sources,
    )


if __name__ == "__main__":
    main()
