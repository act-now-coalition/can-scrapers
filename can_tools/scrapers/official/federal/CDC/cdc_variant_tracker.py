import requests

import pandas as pd
import us

from can_tools.scrapers.base import CMU, ALL_STATES_PLUS_DC
from can_tools.scrapers.official.base import FederalDashboard


STATE_ABBRS = [x.abbr for x in ALL_STATES_PLUS_DC]


class CDCVariantTracker(FederalDashboard):
    has_location = True
    location_type = "state"
    source = "https://www.cdc.gov/coronavirus/2019-ncov/transmission/variant-cases.html"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    def fetch(self):
        fetch_url = "https://www.cdc.gov/coronavirus/2019-ncov/modules/transmission/variant-cases.json"

        return requests.get(fetch_url).json()

    def normalize(self, data):
        # Read in dataframe
        df = pd.DataFrame.from_records(data["data"])
        df.columns = [c.lower() for c in df.columns]
        df = (
            df.rename(columns={"filter": "variable", "cases": "value"})
            .dropna(subset=["state", "variable"])
            .query("state in @STATE_ABBRS")
        )

        # Only keep two columns
        df["location"] = df["state"].map(lambda x: int(us.states.lookup(x).fips))
        df = df.loc[:, ["location", "variable", "value"]]

        # Reshape and add variable information
        crename = {
            "Variant B.1.1.7": CMU(
                category="b117_cases", measurement="cumulative", unit="people"
            ),
            "Variant P.1": CMU(
                category="p1_cases", measurement="cumulative", unit="people"
            ),
            "Variant B.1.351": CMU(
                category="b1351_cases", measurement="cumulative", unit="people"
            ),
        }
        out = self.extract_CMU(df, crename).fillna(0.0)
        out["dt"] = self._retrieve_dt("US/Eastern")
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
            "ethnicity",
            "sex",
            "value",
        ]
        return out.loc[:, cols_2_keep]
