import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class California(TableauDashboard):

    baseurl = "https://public.tableau.com"
    # viewPath = "StateDashboard_16008816705240/6_1CountyTesting"

    def fetch(self):
        return

    def normalize(self):
        testing = self._get_testing()
        # cases = self._get_cases()

        # df = pd.concat([testing, cases], axis=0).sort_values(["dt", "county"])
        df = pd.concat([testing], axis=0).sort_values(["dt"])
        df["vintage"] = self._retrieve_vintage()

        return df

    def _get_testing(self):
        viewPath = "StateDashboard_16008816705240/6_1CountyTesting"
        data = self._scrape_view(viewPath)
        df = data["6.3 County Test - Line (2)"]
        df["dt"] = pd.to_datetime(df["DAY(Test Date)-value"])
        crename = {
            "SUM(Tests)-value": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="specimens",
            ),
        }
        # renamed["county"] = renamed["county"].apply(lambda x: x.lower().capitalize())
        df = (
            df.query("dt != '1970-01-01'")
            .melt(id_vars=["dt"], value_vars=crename.keys())
            .dropna()
        )
        df = self.extract_CMU(df, crename)

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

        return df.loc[:, cols_to_keep]