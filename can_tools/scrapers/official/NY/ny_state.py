import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard


class NewYorkTests(StateDashboard):
    """
    Change log
    ----------
    * 2020-11-09: Updated to reflect new database schema.

    TODO
    ----
    * NY does not report the units that their tests are reported in... -.-
    * No easily accessible cases/deaths data :(
    """

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("New York").fips)
    source = (
        "https://covid19tracker.health.ny.gov/views/NYS-COVID19-Tracker"
        "/NYSDOHCOVID-19Tracker-Map"
    )

    def fetch(self) -> pd.DataFrame:
        """
        Get all county level covid data from NY State dashboard

        Returns
        -------
        df: pd.DataFrame
            A pandas DataFrame containing testing data for all counties in NY
        """
        tests_url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.csv?accessType=DOWNLOAD"

        return pd.read_csv(tests_url, parse_dates=["Test Date"])

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.rename(columns={"Test Date": "dt", "County": "location_name"})

        crename = {
            "New Positives": CMU(
                category="unspecified_tests_positive",
                measurement="new",
                unit="test_encounters",
            ),
            "Total Number of Tests Performed": CMU(
                category="unspecified_tests_total",
                measurement="new",
                unit="test_encounters",
            ),
            "Cumulative Number of Positives": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="test_encounters",
            ),
            "Cumulative Number of Tests Performed": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="test_encounters",
            ),
        }
        out = df.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Determine the category and demographics of each observation
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]
