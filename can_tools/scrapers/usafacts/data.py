from can_tools.scrapers.base import CMU
from typing import Any

import pandas as pd
from can_tools.scrapers.official.base import FederalDashboard

BASEURL = "https://usafactsstatic.blob.core.windows.net/public/data/"


class USAFactsCases(FederalDashboard):
    """
    Downloads USA Fact case data
    Source: https://usafacts.org/visualizations/coronavirus-covid-19-spread-map
    """

    filename = "covid-19/covid_confirmed_usafacts.csv"
    variablename = "cases_total"
    table_name = "usafacts_covid"
    pk = "(vintage, dt, fips, variable_id)"
    data_type = "covid"
    source = "https://usafacts.org/issues/coronavirus/"
    has_location = True

    provider: str = "usafacts"

    category: str = "cases"

    def fetch(self) -> pd.DataFrame:
        return pd.read_csv(BASEURL + self.filename)

    def normalize(self, data: Any) -> pd.DataFrame:
        # Load data from site and move dates from column names to
        # a new variable
        cols = ["countyFIPS", "County Name", "State", "stateFIPS"]
        cols = cols + [c for c in data.columns if c[-2:] in ("20", "21")]

        df = (
            data.loc[:, cols]
            .drop(["County Name", "State"], axis=1)
            .melt(
                id_vars=["countyFIPS", "stateFIPS"], var_name="dt", value_name="value"
            )
        )
        df["dt"] = pd.to_datetime(df["dt"])

        # Drop Wade Hampton Census Area (2270) since it was renamed to
        # Kusilvak and Kusilvak is already included in the data. Also
        # drop Grand Princess Cruise ship (6000)
        df = df.query("(countyFIPS != 2270) & (countyFIPS != 6000)")

        df["value"] = df["value"].astype(str).str.replace(",", "").astype(int)

        # We will report county and state level values -- This means
        # we will group by state fips and then sum... We will then
        # ignore unallocated cases
        df_county = (
            df.query("countyFIPS > 1000")
            .drop("stateFIPS", axis=1)
            .rename(columns={"countyFIPS": "location"})
            .assign(location_type="county")
        )
        df_state = (
            df.groupby(["stateFIPS", "dt"])["value"]
            .sum()
            .reset_index()
            .rename(columns={"stateFIPS": "location"})
            .assign(location_type="state")
        )

        # Stack dfs and then add variable name
        out = pd.concat([df_county, df_state], axis=0, ignore_index=True)
        out["vintage"] = self._retrieve_vintage()

        out["variable"] = "replaceme"
        cmu = CMU(category=self.category, measurement="cumulative", unit="people")
        return out.pipe(self.extract_CMU, cmu={"replaceme": cmu}).drop(
            ["variable"], axis="columns"
        )


class USAFactsDeaths(USAFactsCases):
    """
    Downloads USA Facts death data
    Source: https://usafacts.org/visualizations/coronavirus-covid-19-spread-map
    """

    filename = "covid-19/covid_deaths_usafacts.csv"
    category = "deaths"
