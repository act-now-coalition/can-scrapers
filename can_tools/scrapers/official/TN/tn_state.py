import pandas as pd
import requests
import us

from can_tools.scrapers.base import DatasetBase
from can_tools.scrapers.official.base import StateDashboard


class Tennessee(DatasetBase, StateDashboard):
    has_location = False
    state_fips = int(us.states.lookup("Tennessee").fips)
    source = (
        "https://www.tn.gov/content/tn/health/cedep/ncov/data.html"
        "https://www.tn.gov/health/cedep/ncov/data/downloadable-datasets.html"
    )

    def fetch(self) -> Any:
        # Set url of downloadable dataset
        url = (
            "https://www.tn.gov/content/dam/tn/health/documents/cedep/"
            "novel-coronavirus/datasets/Public-Dataset-Daily-Case-Info.XLSX"
        )
        return requests.get(url)

    def normalize(self, data) -> pd.DataFrame:
        # Read data into 
        df = pd.read_excel(data, parse_dates=["DATE"])

        # Create dictionary for columns to rename
        # Unused variables include: "NEW_CASES", "NEW_CONFIRMED",
        # "NEW_PROBABLE", "NEW_TESTS", "NEW_DEATHS", "NEW_RECOVERED",
        # "TOTAL_RECOVERED", "NEW_ACTIVE", "TOTAL_ACTIVE",
        # "NEW_INACTIVE_RECOVERED", "TOTAL_INACTIVE_RECOVERED",
        # "NEW_HOSP", "TOTAL_HOSP", "TOTAL_DEATHS_BY_DOD",
        # "NEW_DEATHS_BY_DOD".
        crename = {
            "DATE": "dt",
            "TOTAL_CONFIRMED": "cases_confirmed",
            "TOTAL_PROBABLE": "cases_probable",
            "TOTAL_CASES": "cases",
            "TOTAL_DEATHS": "deaths",
            "POS_TESTS": "unspecified_tests_positive",
            "NEG_TESTS": "unspecified_tests_negative",
            "TOTAL_TESTS": "unspecified_tests_total",
        }

        # Rename columns, add fips, melt, drop empty problematic rows, convert value columns, add vintage
        df = df.rename(columns=crename).loc[:, crename.values()]
        df["fips"] = self.state_fips
        out = df.melt(
            id_vars=["dt", "fips"], var_name="variable_name", value_name="value"
        ).dropna()
        out["value"] = out["value"].astype(int)
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        "The best developers write test cases first..."
        # TODO: Create a calculated column and sanity check this against unspecified_tests_total
        #df["unspecified_tests_total_calculated"] = df.eval("unspecified_tests_positive + unspecified_tests_negative")
        return True
