from typing import Dict

import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ETagCacheMixin, FederalDashboard

# Set the maximum number of records to ten million to ensure we collect all rows in the dataset.
# Currently the size of the dataset is 2 million records, so this may need to be updated (increased)
# in the future
SODA_API_RESPONSE_LIMIT = 10000000


class CDCHistoricalTestingDataset(FederalDashboard, ETagCacheMixin):
    has_location = True
    location_type = "county"
    source = "https://data.cdc.gov/Public-Health-Surveillance/United-States-COVID-19-County-Level-of-Community-T/nra9-vzzn/data"
    source_name = "Centers for Disease Control and Prevention"
    # We also collect CDC testing data via the CDCCovidDataTracker class.
    # In order to not overwrite/mix the data sources we use the cdc2 provider instead of cdc.
    # 11/1/21: This is the offical CDC testing dataset and the one that is used by the pipeline downstream.
    provider = "cdc2"

    # SODA API stream has different variable names than the CSV and online dataset -- but the columns appear to be the same.
    variables = {
        "cases_per_100k_7_day_count": CMU(
            category="cases", measurement="rolling_average_7_day", unit="people"
        ),
        "percent_test_results_reported": CMU(
            category="pcr_tests_positive",
            measurement="rolling_average_7_day",
            unit="percentage",
        ),
    }

    def fetch(self) -> pd.DataFrame:
        # select only the columns we care about in order to speed up query
        data = pd.read_json(
            f"https://data.cdc.gov/resource/nra9-vzzn.json?$limit={SODA_API_RESPONSE_LIMIT}"
            "&$select=fips_code,date,cases_per_100k_7_day_count,percent_test_results_reported"
        )
        if len(data) == SODA_API_RESPONSE_LIMIT:
            raise ValueError(
                f"Size of the dataset has reached the currently set API response limit of {SODA_API_RESPONSE_LIMIT}. "
                "Please increase the limit by modifying SODA_API_RESPONSE_LIMIT."
            )
        return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return (
            # remove suppressed values as we have no way to handle them
            data.replace("suppressed", None)
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
                date_column="date",
                locations_to_drop=[2066, 2063, 72888, 72999],
            ).pipe(self._reshape_variables, variable_map=self.variables)
        )
