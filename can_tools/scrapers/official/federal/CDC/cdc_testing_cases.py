from typing import Dict

import pandas as pd
import numpy as np

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ETagCacheMixin, FederalDashboard

# Set the maximum number of records to ten million to ensure we collect all rows in the dataset.
# Currently the size of the dataset is 12 million records, so this may need to be updated (increased)
# in the future
SODA_API_RESPONSE_LIMIT = 20000000


class CDCTestingBase(FederalDashboard, ETagCacheMixin):
    has_location = True
    location_type = "county"
    source_name = "Centers for Disease Control and Prevention"

    cache_file: str
    fetch_url: str
    provider: str
    date_column: str

    # SODA API stream has different variable names than the CSV and online dataset -- but the columns appear to be the same.
    variables = {
        "cases_per_100k_7_day_count": CMU(
            category="cases", measurement="new_7_day", unit="cases_per_100k"
        ),
        "percent_test_results_reported": CMU(
            category="pcr_tests_positive",
            measurement="rolling_average_7_day",
            unit="percentage",
        ),
    }

    # Send URL and filename that Mixin will use to check the etag
    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        ETagCacheMixin.initialize_cache(
            self, cache_url=self.fetch_url, cache_file=self.cache_file
        )
        super().__init__(execution_dt=execution_dt)

    def fetch(self) -> pd.DataFrame:
        # select only the columns we care about in order to speed up query
        data = pd.read_json(
            f"{self.fetch_url}?$limit={SODA_API_RESPONSE_LIMIT}"
            f"&$select=fips_code,{self.date_column},cases_per_100k_7_day_count,percent_test_results_reported"
        )
        if len(data) == SODA_API_RESPONSE_LIMIT:
            raise ValueError(
                f"Size of the dataset has reached the currently set API response limit of {SODA_API_RESPONSE_LIMIT}. "
                "Please increase the limit by modifying SODA_API_RESPONSE_LIMIT."
            )
        return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = (
            # remove suppressed values as we have no way to handle them
            data.replace("suppressed", np.NaN)
            # 02066, Copper River Census Area is a new area established in 2019.
            # We currently do not track it, and instead track Valdez-Cordova Census Area,
            # the census area from which Copper River split.
            # https://en.wikipedia.org/wiki/Copper_River_Census_Area,_Alaska
            # I could not find any info on the Puerto Rico fips codes (72888 and 72999).
            # They are not locations we surface on the website (or have anywhere in our locations)
            # so I've removed them.
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="fips_code",
                date_column=self.date_column,
                locations_to_drop=[2066, 2063, 72888, 72999],
            ).pipe(self._reshape_variables, variable_map=self.variables)
        )

        # There are duplicated rows and multiple rows for a single variable in the dataset.
        data = data.drop_duplicates()
        # If a variable (e.g. a location, date, and variable type) has multiple entries just select the most
        # recent occurrence. There does not seem to be a pattern or a discernably "correct" value.
        # see for context: https://trello.com/c/9xHrKqo4/137-duplicates-in-cdc-testing-dataset-causing-scraper-to-fail
        group_columns = [col for col in data.columns if col != "value"]
        return data.groupby(group_columns).agg({"value": "last"}).reset_index()


class CDCOriginallyPostedTestingDataset(CDCTestingBase):
    source = "https://data.cdc.gov/Public-Health-Surveillance/Weekly-COVID-19-County-Level-of-Community-Transmis/dt66-w6m6/"
    fetch_url = "https://data.cdc.gov/resource/dt66-w6m6.json"
    date_column = "report_date"
    cache_file = "cdc_originally_posted_testing.txt"

    # Getting a little loose with the definition of provider here.
    # This can't be cdc or cdc2 because it will conflict with the rows from the other CDC testing scrapers.
    provider = "cdc_originally_posted"
