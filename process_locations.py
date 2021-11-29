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
import re
import pandas as pd
import prefect

from datetime import date, timedelta
from prefect import Flow, Parameter, task, unmapped
from prefect.engine import signals
from prefect.tasks.control_flow.filter import FilterTask
from typing import List

COVID_DATA_PATH_PREFIX = "./tmp/final/can_scrape_api_covid_us"
GEO_DATA_PATH = "https://media.githubusercontent.com/media/covid-projections/covid-data-model/main/data/geo-data.csv"
POPULATION_DATA_PATH = "https://media.githubusercontent.com/media/covid-projections/covid-data-public/main/data/misc/fips_population.csv"


@task
def location_ids_for(state: str, geo_data_path: str = GEO_DATA_PATH) -> List[str]:
    df = pd.read_csv(geo_data_path)
    df = df[df["state"] == state]
    return df["location_id"].tolist()


@task
def daily_new_cases_for(location_id: str, provider: str, smooth: int) -> float:
    path = f"{COVID_DATA_PATH_PREFIX}_{location_id}.parquet"

    try:
        df = pd.read_parquet(path)
    except FileNotFoundError:
        # TODO: report the error somewhere. Sentry?
        prefect.context.logger.error(f"missing Parquet COVID data for {location_id}")
        raise signals.SKIP()

    # TODO: add ability to back off to other providers?
    df = df[df["provider"] == provider]

    # Filter to the most recent dates.
    # TODO: replace max date with date.today()
    # TODO: what to do if not enough dates to satisfy smoothing?
    # max_date = date.today()
    max_date = df["dt"].max()
    dates = [max_date - timedelta(days=days) for days in range(0, smooth + 1)]
    df = df[df["dt"].isin(dates)]

    df = df[df["variable_name"] == "cases"]
    df = df[df["age"] == "all"]
    df = df[df["ethnicity"] == "all"]
    df = df[df["race"] == "all"]
    df = df[df["sex"] == "all"]

    # TODO: maybe better to do this in pandas? i'm just pulling it out into
    # plain Python data structures since i don't know pandas / existing code
    # well. in the end, this should be replaced with existing modeling code.
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
def sum_numbers(numbers):
    return sum(numbers)


def fips_id_for(location_id):
    return int(re.findall(r"fips:(\d+)", location_id)[0])


@task
def population_of(location_ids: List[str], population_data_path: str):
    fips_ids = []
    for location_id in location_ids:
        try:
            fips_ids.append(fips_id_for(location_id))
        except:
            prefect.context.logger.error(f"missing population data for {location_id}")

    df = pd.read_csv(population_data_path)
    df = df[df["fips"].isin(fips_ids)]
    return df["population"].sum()


@task
def calculate_case_density(new_cases, population):
    case_density = new_cases / population * 100_000

    print(f"new cases: {new_cases}")
    print(f"population: {population}")
    print(f"case density per 100k: {case_density}")

    return case_density


@task
def log_data(data):
    print(data)


def create_flow():
    remove_skipped_tasks = FilterTask(
        filter_func=lambda x: not isinstance(x, signals.SKIP)
    )

    with Flow("ProcessLocations") as flow:
        geo_data_path = Parameter("geo_data_path", default=GEO_DATA_PATH)
        provider = Parameter("provider", default="usafacts")
        population_data_path = Parameter(
            "population_data_path", default=POPULATION_DATA_PATH
        )
        smooth = Parameter("smooth", default=7)
        state = Parameter("state")

        location_ids = location_ids_for(state, geo_data_path)
        daily_new_cases = sum_numbers(
            remove_skipped_tasks(
                daily_new_cases_for.map(
                    location_ids, unmapped(provider), unmapped(smooth)
                )
            )
        )
        population = population_of(location_ids, population_data_path)
        case_density = calculate_case_density(daily_new_cases, population)

        # TODO: output to file instead
        log_data(case_density)

    return flow


def main():
    parser = argparse.ArgumentParser(
        description="Generate basic COVID metrics for the given location(s)"
    )
    parser.add_argument(
        "provider",
        default="usafacts",
        help="data provider to use (e.g. usafacts, cdc, ctp, hts)",
    )
    parser.add_argument(
        "states",
        help="comma-separated list of two-letter state abbreviation(s)",
    )
    args = parser.parse_args()

    states = [state.strip() for state in args.states.split(",")]

    for state in states:
        flow = create_flow()
        flow.run(
            geo_data_path="./geo-data.csv",
            population_data_path="./fips_population.csv",
            provider=args.provider,
            state=state,
        )


if __name__ == "__main__":
    main()
