import us
import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class DelawareCountyData(StateDashboard):
    """
    Fetch county-level covid data from Delaware's database
    """

    # Initialize
    source = "https://myhealthycommunity.dhss.delaware.gov/locations/county-"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Delaware").fips)
    cntys = [["kent", 1], ["new-castle", 3], ["sussex", 5]]

    def fetch(self):
        outDf = pd.DataFrame()
        for county in self.cntys:
            data = pd.read_csv(self.source + county[0] + "/download_covid_19_data")
            data["Date"] = pd.to_datetime(data[["Year", "Month", "Day"]])
            data = data[data["Date"] == data["Date"].max()]
            data = data[data["Age adjusted"] == False]
            data["StatUnit"] = data["Statistic"] + "_" + data["Unit"]
            data.drop(
                columns=[
                    "Year",
                    "Month",
                    "Day",
                    "Location",
                    "Age adjusted",
                    "Date used",
                    "Statistic",
                    "Unit",
                    "County",
                ],
                inplace=True,
            )
            data["location"] = (self.state_fips * 1000) + county[1]
            data.reset_index(inplace=True, drop=True)
            data = pd.pivot_table(
                data,
                values="Value",
                index=["location", "Date"],
                columns="StatUnit",
                aggfunc="first",
            ).reset_index()
            # County-level vaccine data not currently available (Jan22,2021)
            outDf = pd.concat([outDf, data], sort=True, ignore_index=True)
            outDf.fillna(0, inplace=True)

        return outDf

    def normalize(self, data):
        df = data
        crename = {
            "New Positive Cases_people": CMU(
                category="cases", measurement="new", unit="people"
            ),
            "Deaths_people": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "Total Tests_tests": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "Positive Tests_tests": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            "Cumulative Number of Positive Cases_people": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "Persons Tested Negative_people": CMU(
                category="unspecified_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            "Total Persons Tested_people": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
        }

        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out.rename(columns={"Name": "variable"}, inplace=True)
        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
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
        return out.loc[:, cols_to_keep]
