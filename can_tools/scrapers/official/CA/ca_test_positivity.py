import us
import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class CaliforniaTestPositivity(StateDashboard):
    has_location = False
    source = (
        "https://data.chhs.ca.gov/dataset/covid-19-time-series-metrics-by-county-and-state/"
        "resource/046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a"
    )
    source_name = "California Health & Human Services Agency"
    state_fips = int(us.states.lookup("California").fips)
    location_type = "county"

    url = (
        "https://data.chhs.ca.gov/dataset/f333528b-4d38-4814-bebb-12db1f10f535/resource/"
        "046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a/download/covid19cases_test.csv"
    )

    variables = {
        "cumulative_positive_tests": variables.CUMULATIVE_POSITIVE_TEST_SPECIMENS,
        "cumulative_total_tests": CMU(
            category="pcr_tests_total",
            measurement="cumulative",
            unit="specimens",
        ),
        "total_tests": CMU(
            category="pcr_tests_total",
            measurement="new",
            unit="specimens",
        ),
        "positive_tests": CMU(
            category="pcr_tests_positive",
            measurement="new",
            unit="specimens",
        ),
    }

    def fetch(self) -> pd.DataFrame:
        return pd.read_csv(self.url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        non_counties = [
            "Unknown",
            "California",
            "Out of state",
        ]
        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="area",
            date_column="date",
            location_names_to_drop=non_counties,
        )
        return self._reshape_variables(data, self.variables)
