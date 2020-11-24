import requests

import pandas as pd
import us

from can_tools.scrapers.official.base import CountyDashboard
from can_tools.scrapers.base import CMU, DatasetBase


class CDCCovidDataTracker(CountyDashboard, DatasetBase):
    has_location = True
    geo_type = "county"
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"

    def __init__(self, execution_dt: pd.Timestamp):
        super().__init__(execution_dt)

    def fetch(self):
        fetcher_url = (
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/"
            "getAjaxData?id=integrated_county_timeseries_state_{}_external"
        )

        # Iterate through the states collecting the time-series data
        data = []
        for state in us.STATES[:5]:
            # Update url to get the particular state we're working with
            res = requests.get(fetcher_url.format(state.abbr.lower()))

            data.append(res.json())

        return data

    def normalize(self, data):
        # Read data in
        data_key = "integrated_county_timeseries_external_data"
        df = pd.concat(
            [pd.DataFrame.from_records(x[data_key]) for x in data],
            axis=0,
            ignore_index=True,
        ).rename(columns={"fips_code": "location"})

        # Set datetime to the end of report window -- We'll be reporting
        # backwards looking windows. "date" and "report_date_window" are
        # the same value as of 2020-11-21
        df["dt"] = pd.to_datetime(df["date"])

        crename = {
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

        # Reshape and add variable information
        out = df.melt(id_vars=["dt", "location"], value_vars=crename.keys()).dropna()
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()

        cols_2_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]
        return out.loc[:, cols_2_keep]
