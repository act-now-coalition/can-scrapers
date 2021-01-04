from typing import Any

import pandas as pd
import us
import requests

from can_tools.scrapers.base import CMU, DatasetBase
from can_tools.scrapers.official.base import SODA



class CTCountyDeathHospitalizations(SODA, DatasetBase):
    source = "https://data.ct.gov/bfnu-rgqt.json"
    state_fips = int(us.states.lookup("Connecticut").fips)
    baseurl = "https://data.ct.gov/"
    has_location = False
    location_type = "county"
    cdh_id = "bfnu-rgqt"
    state_fips = int(us.states.lookup("Connecticut").fips)
    source = "https://data.ct.gov/resource/bfnu-rgqt.json"
    def fetch(self):
        cdh = self.get_dataset(self.cdh_id)

        return cdh
    def normalize(self, data) -> pd.DataFrame:
        # fips_df = pd.DataFrame.from_dict(self.state_fips)
        cdh_rename = {
            "dateupdated": "dt",
            "county": "location_name",
            "totalcases": "cases_total",
            "totaldeaths": "deaths_total",
            "hospitalization": "hospital_beds_in_use_covid_total",
        }
        cdh = data.rename(columns=cdh_rename)
        cdh["dt"] = pd.to_datetime(cdh["dt"])
        cdh = cdh.reindex(columns=cdh_rename.values())
        for c in [c for c in cdh_rename.values() if (c != "dt") and (c != "location_name")]:
            cdh[c] = pd.to_numeric(cdh.loc[:, c])
        crename = {
            "cases_total": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "deaths_total": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "hospital_beds_in_use_covid_total": CMU(
                            category="hospital_beds_in_use_covid",
                            measurement="current",
                            unit="beds",
                        ),
        }
        for c in [c for c in cdh.columns if c not in (crename.keys()) and c != "dt" and c != "location_name"]:
            cdh = cdh.drop(c, 1)
        out = cdh.melt(id_vars=["dt", "location_name"], value_vars=crename.keys()).dropna()

        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        print(out.columns)
        return out

class CTCountyTests(SODA, DatasetBase):
    baseurl = "https://data.ct.gov/"
    test_id = "qfkt-uahj"
    state_fips = int(us.states.lookup("Connecticut").fips)
    has_location = False
    location_type = "county"
    source = "https://data.ct.gov/resource/qfkt-uahj.json"

    def fetch(self):
        tests = self.get_dataset(self.test_id)

        return tests
    def normalize(self, data: Any) -> pd.DataFrame:
        # Get raw dataframe
        tests_rename = {
            "date": "dt",
            "county": "location_name",
            "number_of_ag_tests": "tests_ag_total",
            "number_of_ag_positives": "positive_tests_ag_total",
            "number_of_ag_negatives": "negative_tests_ag_total",
            "number_of_pcr_tests": "tests_pcr_total",
            "number_of_pcr_positives": "positive_tests_pcr_total",
            "number_of_pcr_negatives": "negative_tests_pcr_total",
        }
        tests = data.rename(columns=tests_rename)

        tests["dt"] = pd.to_datetime(tests["dt"])

        tests = tests.reindex(columns=tests_rename.values())
        for c in [c for c in tests_rename.values() if "tests" in c]:
            tests[c] = pd.to_numeric(tests.loc[:, c])
        tests = tests.loc[~tests["dt"].isna(), :]
        tests = tests.query("location_name != 'Pending address validation'")

        crename = {
            "tests_ag_total": CMU(
                category="antigen_tests_total", measurement="new", unit="specimens"
            ),
            "positive_tests_ag_total": CMU(
                category="antigen_tests_positive", measurement="new", unit="specimens"
            ),
            "negative_tests_ag_total": CMU(
                            category="antigen_tests_negative",
                            measurement="new",
                            unit="specimens",
                        ),
            "tests_pcr_total": CMU(
                category="pcr_tests_total", measurement="new", unit="specimens"
            ),
            "positive_tests_pcr_total": CMU(
                category="pcr_tests_positive", measurement="new", unit="specimens"
            ),
            "negative_tests_pcr_total": CMU(
                category="pcr_tests_negative",
                measurement="new",
                unit="specimens",
            ),
        }
        for c in [c for c in tests.columns if c not in (crename.keys()) and c != "dt" and c != "location_name"]:
            tests = tests.drop(c, 1)
        out = tests.melt(id_vars=["dt", "location_name"], value_vars=crename.keys()).dropna()
        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        return out


class CTState(SODA, DatasetBase):
    execution_dt = pd.Timestamp.now()
    baseurl = "https://data.ct.gov/"
    resource_id = "rf3k-f8fg"
    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Connecticut").fips)
    source = "https://data.ct.gov/resource/rf3k-f8fg.json"
    def fetch(self):
        stateCaseTests = self.get_dataset(self.resource_id)
        return stateCaseTests

    def normalize(self, data: Any) -> pd.DataFrame:
        
        data["date"] = pd.to_datetime(data["date"])
        data = data.rename(columns = {"date": 'dt'})
        data["location"] = self.state_fips
        for c in [c for c in data.columns if c != "dt" and c != "state"]:
            data[c] = pd.to_numeric(data.loc[:, c])
        data = data.loc[~data["dt"].isna(), :]
        crename = {
            "covid_19_tests_reported": CMU(
                category="unspecified_tests_total", measurement="cumulative", unit="specimens"
            ),
            "confirmedcases": CMU(
                category="unspecified_tests_positive", measurement="cumulative", unit="unique_people"
            ),
            "totalcases": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "totaldeaths": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
            ),
            "hospitalizedcases": CMU(
                category="hospital_beds_in_use_covid", measurement="cumulative", unit="beds"
            )
        }
        for c in [c for c in data.columns if c not in (crename.keys()) and c != "dt" and c != "location"]:
            data = data.drop(c, 1)
        out = data.melt(id_vars=["dt", "location"], value_vars=crename.keys()).dropna()
        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        print(out.columns)
        return out
