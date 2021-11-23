# TODO: The prefect and pyarrow packages are not part of either Pipfile or
# requirements.txt. They're currently manually installed in production via
# services/prefect/setup_gcp_instance.sh. We should probably fix that.

'''
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
'''

import logging
import pandas as pd
import sys

from prefect import task, Flow, Parameter

logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG,
        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s')

@task
def fetch_parquet_data(path):
    return pd.read_parquet(path)

@task
def log_data(data):
    logging.info(data.columns)

def main():
    with Flow('foo') as flow:
        path = Parameter('path', default='https://storage.googleapis.com/can-scrape-outputs/final/can_scrape_api_covid_us.parquet')
        data = fetch_parquet_data(path)

        log_data(data)

    flow.run(path='can_scrape_api_covid_us.parquet')

if __name__ == '__main__':
    main()

'''
data_remote_path = 'https://storage.googleapis.com/can-scrape-outputs/final/can_scrape_api_covid_us.parquet'
try:
    logging.info(f'Reading data from {data_local_path}')
    dataframe = pd.read_parquet(data_local_path)
except:
    logging.info(f'Reading data from {data_remote_path}')
    dataframe = pd.read_parquet(data_remote_path)

logging.info('done')
'''
