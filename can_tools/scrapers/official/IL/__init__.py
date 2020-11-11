import pandas as pd
import requests
import us

from ... import DatasetBaseNoDate
from ..base import CMU, CountyData


class IllinoisHistorical(DatasetBaseNoDate, CountyData):
    has_location = False
    source = "https://www.dph.illinois.gov/covid19/covid19-statistics"
    state_fips = int(us.states.lookup("Illinois").fips)

    def _get_js(self, url: str) -> dict:
        res = requests.get(url)
        if not res.ok:
            raise ValueError("Could not request data from {url}".format(url))

        return res.json()

    def get(self):
        url = "https://www.dph.illinois.gov/sitefiles/COVIDHistoricalTestResults.json?nocache=1"
        js = self._get_js(url)
        cats = {
            "confirmed_cases": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people",),
            "total_tested": CMU(
                category="antigen_pcr_tests_total",
                measurement="cumulative",
                unit="unknown",
            ),
        }

        to_cat = []
        for date_data in js["historical_county"]["values"]:
            dt = pd.to_datetime(date_data["testDate"])
            to_cat.append(
                pd.DataFrame(date_data["values"])
                .rename(columns={"County": "county"})
                .melt(id_vars=["county"], value_vars=list(cats.keys()))
                .pipe(self.extract_cat_measurement_unit, cats)
                .assign(dt=dt, vintage=self._retrieve_vintage())
                .drop(["variable"], axis=1)
            )

        unique_cols = [
            "vintage",
            "dt",
            "county",
            "category",
            "unit",
            "measurement",
            "age",
            "race",
            "sex",
        ]

        return (
            pd.concat(to_cat, ignore_index=True, sort=True)
            .drop_duplicates(subset=unique_cols)
        )


class IllinoisDemographics(IllinoisHistorical, DatasetBaseNoDate):
    def _handle_demo_subset(self, df, orig, final):
        "orig is demo colname in df, final is what we want it to be"

        cats = {
            "count": CMU(category="cases", measurement="cumulative", unit="people",),
            "tested": CMU(
                category="antigen_pcr_tests_total",
                measurement="cumulative",
                unit="unknown",
            ),
        }

        temp = "replaceme"

        return (
            df.rename(columns={orig: temp})
            .melt(value_vars=cats.keys(), id_vars=[temp])
            .assign(dt=self._retrieve_dt(), vintage=self._retrieve_vintage())
            .pipe(self.extract_cat_measurement_unit, cats)
            .drop([final, "variable"], axis=1)
            .rename(columns={temp: final})
        )

    def get(self):
        url = "https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetCountyDemographics"
        js = self._get_js(url)

        # process age
        parts = []
        for county in js["county_demographics"]:
            name = county["County"]
            cases = county["confirmed_cases"]
            tests = county["total_tested"]

            parts.append(
                self._handle_demo_subset(
                    pd.DataFrame(county["demographics"]["age"]), "age_group", "age",
                ).assign(county=name)
            )
            parts.append(
                self._handle_demo_subset(
                    pd.DataFrame(county["demographics"]["gender"]),
                    "description",
                    "sex",
                ).assign(county=name)
            )
            parts.append(
                self._handle_demo_subset(
                    pd.DataFrame(county["demographics"]["race"]), "description", "race",
                ).assign(county=name)
            )

        out = pd.concat(parts, ignore_index=True)
        out["race"] = out["race"].str.lower()
        out["sex"] = out["sex"].str.lower()
        out["age"] = out["age"].str.lower()
        return out
