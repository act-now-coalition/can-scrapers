import us
import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class DelawareData(StateDashboard):
    """
    Fetch county-level covid data from Delaware's database
    """

    # Initialize
    source = "https://myhealthycommunity.dhss.delaware.gov"
    has_location = True
    location_type = ""
    state_fips = int(us.states.lookup("Delaware").fips)
    locs = [
        ["state", "state", 0],
        ["county", "kent", 1],
        ["county", "new-castle", 3],
        ["county", "sussex", 5]
    ]

    def fetch(self):
        dfs = []
        for loc in self.locs:
            if loc[0] == "county":
                url = self.source + f"/locations/county-{loc[1]}/download_covid_19_data"
                _locnumber = (self.state_fips * 1000) + loc[-1]
            else:
                url = self.source + f"/locations/state/download_covid_19_data"
                _locnumber = self.state_fips
            _loctype = loc[0]
            data = pd.read_csv(url)

            # Set date
            data["Date"] = pd.to_datetime(data[["Year", "Month", "Day"]])

            # Only keep non-age-adjusted, no rates per number of people
            data = data[data["Age adjusted"] == False]
            data = data[~data["Unit"].str.contains("rate")]

            # Mix statistic column and unit column and drop extra information
            data["StatUnit"] = data["Statistic"] + "_" + data["Unit"]
            data = data.drop(
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
                ]
            )
            data["location"] = _locnumber
            data["location_type"] = _loctype

            # Reshape as desired
            dfs.append(data)
            # County-level vaccine data not currently available (Jan 22, 2021)

        outDf = pd.concat(dfs, axis=0, ignore_index=True, sort=True)
        outDf = outDf.fillna(0)

        return outDf

    def normalize(self, data):
        df = data.copy()

        crename = {
            "Cumulative Number of Positive Cases_people": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
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
            # TODO: Add demographic information from county data
        }

        out = (
            df.rename(
                columns={"StatUnit": "variable", "Value": "value"}
            ).assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            ).query(
                "variable in @crename.keys()"
            ).dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "location_type",
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
