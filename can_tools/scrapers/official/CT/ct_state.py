from typing import Any

import pandas as pd
import us
import requests

from can_tools.scrapers.base import CMU, DatasetBase
from can_tools.scrapers.official.base import SODA


class CTCountyDeathHospitalizations(SODA, DatasetBase):
    baseurl = "https://data.ct.gov/"
    has_location = False
    location_type = "county"
    cdh_id = "bfnu-rgqt"
    state_fips = int(us.states.lookup("Connecticut").fips)
    source = "https://data.ct.gov/resource/bfnu-rgqt.json"
    source_name = "Official Connecticut State Government Website"

    def fetch(self):
        cdh = self.get_dataset(self.cdh_id)

        return cdh

    def normalize(self, data) -> pd.DataFrame:
        cdh_rename = {"dateupdated": "dt", "county": "location_name"}
        cdh = data.rename(columns=cdh_rename)
        cdh["dt"] = pd.to_datetime(cdh["dt"])
        crename = {
            "totalcases": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "totaldeaths": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "hospitalization": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
        }
        out = cdh.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()
        out["value"] = pd.to_numeric(out.loc[:, "value"])

        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        return out


class CTCountyTests(SODA, DatasetBase):
    baseurl = "https://data.ct.gov/"
    test_id = "qfkt-uahj"
    state_fips = int(us.states.lookup("Connecticut").fips)
    has_location = False
    location_type = "county"
    source = "https://data.ct.gov/resource/qfkt-uahj.json"
    source_name = "Official Connecticut State Government Website"

    def fetch(self):
        tests = self.get_dataset(self.test_id)

        return tests

    def normalize(self, data: Any) -> pd.DataFrame:
        # Get raw dataframe
        tests_rename = {"date": "dt", "county": "location_name"}
        tests = data.rename(columns=tests_rename)

        tests["dt"] = pd.to_datetime(tests["dt"])
        tests = tests.loc[~tests["dt"].isna(), :]
        tests = tests.query("location_name != 'Pending address validation'")

        crename = {
            "number_of_ag_tests": CMU(
                category="antigen_tests_total", measurement="new", unit="specimens"
            ),
            "number_of_ag_positives": CMU(
                category="antigen_tests_positive", measurement="new", unit="specimens"
            ),
            "number_of_ag_negatives": CMU(
                category="antigen_tests_negative",
                measurement="new",
                unit="specimens",
            ),
            "number_of_pcr_tests": CMU(
                category="pcr_tests_total", measurement="new", unit="specimens"
            ),
            "number_of_pcr_positives": CMU(
                category="pcr_tests_positive", measurement="new", unit="specimens"
            ),
            "number_of_pcr_negatives": CMU(
                category="pcr_tests_negative",
                measurement="new",
                unit="specimens",
            ),
        }

        out = tests.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()
        out["value"] = pd.to_numeric(out.loc[:, "value"])
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
    source_name = "Official Connecticut State Government Website"

    def fetch(self):
        stateCaseTests = self.get_dataset(self.resource_id)
        return stateCaseTests

    def normalize(self, data: Any) -> pd.DataFrame:

        data["date"] = pd.to_datetime(data["date"])
        data = data.rename(columns={"date": "dt"})
        data["location"] = self.state_fips
        data = data.loc[~data["dt"].isna(), :]
        crename = {
            "covid_19_tests_reported": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "confirmedcases": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="unique_people",
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
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="beds",
            ),
        }
        out = data.melt(id_vars=["dt", "location"], value_vars=crename.keys()).dropna()
        out["value"] = pd.to_numeric(out.loc[:, "value"])
        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        return out
