from typing import Any

import pandas as pd
from urllib.request import Request, urlopen
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ETagCacheMixin, FederalDashboard
import abc

BASEURL = "https://static.usafacts.org/public/data/"


class USAFactsBase(FederalDashboard):
    """
    Base class for downloading USA Facts data
    Source: https://usafacts.org/visualizations/coronavirus-covid-19-spread-map
    """

    __metaclass__ = abc.ABCMeta

    variablename = "cases_total"
    table_name = "usafacts_covid"
    pk = "(vintage, dt, fips, variable_id)"
    data_type = "covid"
    source = "https://usafacts.org/issues/coronavirus/"
    source_name = "USAFacts"
    has_location = True
    location_type = ""

    provider: str = "usafacts"

    @abc.abstractproperty
    def filename(self):
        pass

    @abc.abstractproperty
    def category(self):
        pass

    def fetch(self) -> pd.DataFrame:
        req = Request(BASEURL + self.filename, headers={"User-Agent": "Mozilla/5.0"})
        return pd.read_csv(urlopen(req))

    def normalize(self, data: Any) -> pd.DataFrame:
        # Make lowercase so they can't change capitalization on us
        data.columns = [c.lower() for c in data.columns]

        df = data.drop(["county name", "state"], axis=1).melt(
            id_vars=["countyfips", "statefips"], var_name="dt", value_name="value"
        )
        df["dt"] = pd.to_datetime(df["dt"])

        # Drop Wade Hampton Census Area (2270) since it was renamed to
        # Kusilvak and Kusilvak is already included in the data. Also
        # drop Grand Princess Cruise ship (6000)
        df = df.query("(countyfips != 2270) & (countyfips != 6000)")

        df["value"] = df["value"].astype(str).str.replace(",", "").astype(int)

        # We will report county and state level values -- This means
        # we will group by state fips and then sum... We will then
        # ignore unallocated cases
        df_county = (
            df.query("countyfips > 1000")
            .drop("statefips", axis=1)
            .rename(columns={"countyfips": "location"})
            .assign(location_type="county")
        )
        df_state = (
            df.groupby(["statefips", "dt"])["value"]
            .sum()
            .reset_index()
            .rename(columns={"statefips": "location"})
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

    def validate(self, df, df_hist):
        # Overriding validate method to not fail for usa facts because
        # county cumulative case/death reporting has dips because of local corrections.
        # TODO(chris): Add option to skip increasing over time validate check.
        return


class USAFactsCases(USAFactsBase, ETagCacheMixin):
    """
    Downloads USA Facts cases data
    Source: https://usafacts.org/visualizations/coronavirus-covid-19-spread-map
    """

    # Send URL and filename that Mixin will use to check the etag
    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        self._filename = "covid-19/covid_confirmed_usafacts.csv"
        self._category = "cases"
        ETagCacheMixin.initialize_cache(
            self, cache_url=BASEURL + self.filename, cache_file="usa_facts_cases.txt"
        )
        super().__init__(execution_dt=execution_dt)

    @property
    def filename(self):
        return self._filename

    @property
    def category(self):
        return self._category


class USAFactsDeaths(USAFactsBase, ETagCacheMixin):
    """
    Downloads USA Facts death data
    Source: https://usafacts.org/visualizations/coronavirus-covid-19-spread-map
    """

    # Send URL and filename that Mixin will use to check the etag
    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        self._filename = "covid-19/covid_deaths_usafacts.csv"
        self._category = "deaths"
        ETagCacheMixin.initialize_cache(
            self, cache_url=BASEURL + self.filename, cache_file="usa_facts_deaths.txt"
        )
        super().__init__(execution_dt=execution_dt)

    @property
    def filename(self):
        return self._filename

    @property
    def category(self):
        return self._category
