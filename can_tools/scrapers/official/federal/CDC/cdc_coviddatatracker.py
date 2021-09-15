import pathlib
from typing import Dict

import pandas as pd
import requests
import us
import multiprocessing

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard


class CDCCovidDataTracker(FederalDashboard):
    has_location = True
    location_type = "county"
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    variables = {
        "new_cases_7_day_rolling_average": CMU(
            category="cases", measurement="rolling_average_7_day", unit="people"
        ),
        "new_deaths_7_day_rolling_average": CMU(
            category="deaths", measurement="rolling_average_7_day", unit="people"
        ),
        "percent_new_test_results_reported_positive_7_day_rolling_average": CMU(
            category="pcr_tests_positive",
            measurement="rolling_average_7_day",
            unit="percentage",
        ),
        "new_test_results_reported_7_day_rolling_average": CMU(
            category="pcr_tests_total",
            measurement="rolling_average_7_day",
            unit="specimens",  # TODO: Need to ensure this is actually specimens!
        ),
    }

    def __init__(self, *args, state=None, **kwargs):
        self.state = us.states.lookup(state) if state else None
        super().__init__(*args, **kwargs)

    def _filepath(self, raw: bool) -> pathlib.Path:
        # Overriding _filepath to support saving individual files for each state
        path = super()._filepath(raw)
        if not self.state:
            return path

        root = path.parent / f"{self.state.abbr}.{path.name}"
        return root

    @staticmethod
    def _county_request(url: str) -> requests.models.Response:
        return requests.get(url)

    def fetch(self):
        # reset exceptions
        self.exceptions = []
        fetcher_url = (
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/"
            "getAjaxData?id=integrated_county_timeseries_fips_{}_external"
        )

        if self.state:
            counties = self._retrieve_counties(state=self.state, fips=True)
        else:
            raise ValueError("please specify state to fetch data for")

        urls = [fetcher_url.format(county) for county in counties]
        pool = multiprocessing.Pool(processes=8)  # choose 8 processes arbitrarily
        responses = pool.map(self._county_request, urls)

        bad_idx = [i for (i, r) in enumerate(responses) if not r.ok]
        if len(bad_idx):
            bad_urls = "\n".join([urls[i] for i in bad_idx])
            raise ValueError("Failed for these urls:\n{}".format(bad_urls))

        return [r.json() for r in responses]

    def normalize(self, data):
        # Read data in
        data_key = "integrated_county_timeseries_external_data"
        df = pd.concat(
            [pd.DataFrame.from_records(x[data_key]) for x in data],
            axis=0,
            ignore_index=True,
        )

        # We have no way to handle suppressed entries, so remove them
        df = self._rename_or_add_date_and_location(
            df, location_column="fips_code", date_column="date"
        ).replace({"suppressed": None})

        df = self._reshape_variables(df, self.variables)
        return df
