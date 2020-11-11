import pandas as pd
import requests
import us

from ...base import CMU, DatasetBaseNoDate
from ..base import CountyData


class NYCounty(DatasetBaseNoDate, CountyData):
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
    state_fips = int(us.states.lookup("New York").fips)
    source = (
        "https://covid19tracker.health.ny.gov/views/NYS-COVID19-Tracker"
        "/NYSDOHCOVID-19Tracker-Map"
    )

    def get(self):
        tests = self._get_tests_data()

        out = pd.concat([tests], axis=0, ignore_index=True)
        out["vintage"] = self._retrieve_vintage()

        return out

    def _get_tests_data(self):
        tests_url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.csv?accessType=DOWNLOAD"

        df = pd.read_csv(tests_url, parse_dates=["Test Date"]).rename(
            columns={
                "Test Date": "dt",
                "County": "county"
            }
        )

        crename = {
            "New Positives": CMU(
                category="unspecified_tests_positive",
                measurement="new",
                unit="test_encounters"
            ),
            "Total Number of Tests Performed": CMU(
                category="unspecified_tests_total",
                measurement="new",
                unit="test_encounters"
            ),
            "Cumulative Number of Positives": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="test_encounters"
            ),
            "Cumulative Number of Tests Performed": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="test_encounters"
            )
        }
        out = df.melt(
            id_vars=["dt", "county"], value_vars=crename.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Determine the category and demographics of each observation
        out = self.extract_cat_measurement_unit(out, crename)

        cols_to_keep = [
            "dt", "county", "category", "measurement", "unit",
            "age", "race", "sex", "value"
        ]

        return out.loc[:, cols_to_keep]
