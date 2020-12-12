import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard

class TennesseeCasesDeathsTests(CMU, StateDashboard):
    """
    Fetch state level covid data from Tennessee state dashboard
    """
    source = (
        "https://www.tn.gov/content/tn/health/cedep/ncov/data.html"
        "https://www.tn.gov/health/cedep/ncov/data/downloadable-datasets.html"
    )
    state_fips = int(us.states.lookup("Tennessee").fips)
    has_location = True
    location_type = "state"

    def fetch(self) -> requests.Response:
        # Set url of downloadable dataset
        url = (
            "https://www.tn.gov/content/dam/tn/health/documents/cedep/"
            "novel-coronavirus/datasets/Public-Dataset-Daily-Case-Info.XLSX"
        )
        request = requests.get(url)
        return request.content

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data, parse_dates=["DATE"])

        # Create dictionary for columns to rename
        # Unused variables include: "TOTAL_CASES", "NEW_CASES",
        # "NEW_CONFIRMED", "NEW_PROBABLE", "NEW_TESTS", "NEW_RECOVERED",
        # "TOTAL_RECOVERED", "NEW_ACTIVE", "TOTAL_ACTIVE",
        # "NEW_INACTIVE_RECOVERED", "TOTAL_INACTIVE_RECOVERED",
        # "NEW_HOSP", "TOTAL_HOSP", "TOTAL_DEATHS_BY_DOD",
        # "NEW_DEATHS_BY_DOD".
        crename = {
            "TOTAL_CONFIRMED": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "TOTAL_PROBABLE": CMU(
                category="cases", measurement="new", unit="people"
            ),
            "TOTAL_DEATHS": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "NEW_DEATHS": CMU(
                category="deaths", measurement="new", unit="people"
            ),
            "POS_TESTS": CMU(
                category="unspecified_tests_positive", measurement="cumulative", unit="specimens"
            ),
            "NEG_TESTS": CMU(
                category="unspecified_tests_negative", measurement="cumulative", unit="specimens"
            ),
            "TOTAL_TESTS": CMU(
                category="unspecified_tests_total", measurement="cumulative", unit="specimens"
            ),
        }

        # Rename columns
        df = df.rename(columns={"DATE": "dt"})

        # Move things into long format
        df = df.melt(
            id_vars=["dt"], value_vars=crename.keys()
        ).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename)

        # Determine what columns to keep
        cols_to_keep = [
            "dt",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        # Drop extraneous columns
        out = df.loc[:, cols_to_keep]

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["location"] = self.state_fips
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        "The best developers write test cases first..."
        # TODO: Create a calculated column and sanity check this against unspecified_tests_total
        #df["unspecified_tests_total_calculated"] = df.eval("unspecified_tests_positive + unspecified_tests_negative")
        return True
