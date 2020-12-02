import grequests
import pandas as pd
import us

from can_tools.scrapers.official.base import FederalDashboard
from can_tools.scrapers.base import CMU


class CDCCovidDataTracker(FederalDashboard):
    has_location = True
    location_type = "county"
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"

    def __init__(self, , *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exceptions = []

    def exception(self, request, exception):
        print("Problem: {}: {}".format(request.url, exception))
        self.exceptions.append((request.url, exception))

    def fetch(self):
        # reset exceptions
        self.exceptions = []
        fetcher_url = (
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/"
            "getAjaxData?id=integrated_county_timeseries_state_{}_external"
        )

        # Iterate through the states collecting the time-series data
        urls = [fetcher_url.format(state.abbr.lower()) for state in us.STATES]
        responses = grequests.map(
            (grequests.get(u) for u in urls), size=5, exception_handler=self.exception
        )
        out = [x.json() for x in responses if x is not None]
        if len(self.exceptions):
            raise ValueError("Got some exceptions: {}".format(self.exceptions))

        return out

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
