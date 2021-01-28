import pandas as pd
from pandas.core.frame import DataFrame

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard

from typing import Any
import us


class CaliforniaTesting(TableauDashboard):

    source = "https://covid19.ca.gov/state-dashboard/"
    location_type = "county"
    state_fips = int(us.states.lookup("California").fips)
    has_location = False
    baseurl = "https://public.tableau.com"
    viewPath = "StateDashboard_16008816705240/6_1CountyTesting"
    filterFunctionName = "[federated.1vtltxr1fwdaou18i20yk06cu6zn].[none:COUNTY:nk]"

    counties = [
        "Alameda",
        "Alpine",
        "Amador",
        "Butte",
        "Calaveras",
        "Colusa",
        "Contra Costa",
        "Del Norte",
        "El Dorado",
        "Fresno",
        "Glenn",
        "Humboldt",
        "Imperial",
        "Inyo",
        "Kern",
        "Kings",
        "Lake",
        "Lassen",
        "Los Angeles",
        "Madera",
        "Marin",
        "Mariposa",
        "Mendocino",
        "Merced",
        "Modoc",
        "Mono",
        "Monterey",
        "Napa",
        "Nevada",
        "Orange",
        "Placer",
        "Plumas",
        "Riverside",
        "Sacramento",
        "San Benito",
        "San Bernardino",
        "San Diego",
        "San Francisco",
        "San Joaquin",
        "San Luis Obispo",
        "San Mateo",
        "Santa Barbara",
        "Santa Clara",
        "Santa Cruz",
        "Shasta",
        "Sierra",
        "Siskiyou",
        "Solano",
        "Sonoma",
        "Stanislaus",
        "Sutter",
        "Tehama",
        "Trinity",
        "Tulare",
        "Tuolumne",
        "Ventura",
        "Yolo",
        "Yuba",
    ]

    def fetch(self, test=False) -> Any:
        to_cat = []
        for countyName in self.counties:
            self.filterFunctionValue = countyName
            countyData = self.get_tableau_view()
            countyTestCounts = self._get_test_counts(countyData)
            countyTestPositivity = self._get_test_positivity(countyData)
            currentCounty = pd.concat(
                [countyTestCounts, countyTestPositivity], axis=0
            ).sort_values(["dt"])
            currentCounty["location_name"] = countyName
            to_cat.append(currentCounty)
            if test and countyName == "Contra Costa":
                # If test, only use first 7 counties
                break
        return pd.concat(to_cat, axis=0).sort_values(["dt", "location_name"])

    def normalize(self, data) -> pd.DataFrame:
        data.loc[:, "value"] = pd.to_numeric(data["value"])
        data["vintage"] = self._retrieve_vintage()
        return data.drop(["variable"], axis="columns")

    def _get_test_counts(self, countyData):
        df = countyData["6.3 County Test - Line (2)"]
        df["dt"] = pd.to_datetime(df["DAY(Test Date)-value"])
        crename = {
            "SUM(Tests)-value": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="specimens",
            ),
        }
        df = (
            df.query("dt >= '2020-01-01'")
            .melt(id_vars=["dt"], value_vars=crename.keys())
            .dropna()
        )
        df = self.extract_CMU(df, crename)

        return df

    def _get_test_positivity(self, countyData):
        df = countyData["6.4 County Pos - Line (2)"].rename(
            columns={"Measure Names-alias": "countyOrState"}
        )
        df = df[~df.countyOrState.str.contains("Statewide")]
        df["dt"] = pd.to_datetime(df["DAY(Test Date)-value"])
        crename = {
            "Measure Values-value": CMU(
                category="pcr_tests_positive",
                measurement="rolling_average_14_day",
                unit="percentage",
            ),
        }
        df = (
            df.query("dt >= '2020-01-01'")
            .melt(id_vars=["dt"], value_vars=crename.keys())
            .dropna()
        )
        df = self.extract_CMU(df, crename)

        return df
