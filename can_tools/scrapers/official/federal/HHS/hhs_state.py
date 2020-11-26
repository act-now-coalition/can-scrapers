import bs4 as bs
import requests

from io import StringIO
from typing import Dict

import pandas as pd
import us

from can_tools.scrapers.official.base import FederalDashboard
from can_tools.scrapers.base import CMU, DatasetBase


class HHSTimeSeries(FederalDashboard, DatasetBase):
    has_location = True
    location_type = "state"
    source = (
        "https://healthdata.gov/dataset/"
        "covid-19-estimated-patient-impact-and-hospital-capacity-state"
    )

    def __init__(self, execution_dt: pd.Timestamp):
        super().__init__(execution_dt)

    def fetch(self):
        # Download page html and turn into soup
        res = requests.get(self.source)
        soup = bs.BeautifulSoup(res.text, features="lxml")

        # Filter through information to find download links
        dls = [x for x in soup.find_all(name="a") if x.text.strip() == "Download"]

        categories = ["inpatient_all", "inpatient_covid", "icu"]
        data = {}
        for category in categories:
            dl = [x for x in dls if category in x["href"]][0]
            dl_res = requests.get(dl["href"])

            data[category] = dl_res.text

        return data

    def normalize_helper(self, data: str, crename: Dict[str, CMU]) -> pd.DataFrame:
        "Helper function for repeated operations in data reading"
        # Read the dataframe from the string csv
        df = pd.read_csv(StringIO(data))
        df.columns = [x.lower().strip() for x in df.columns]

        # TODO: wtf is CW an abbreviation for?
        df = df.query("state != 'CW'")

        # Set date and fips code
        df.loc[:, "dt"] = pd.to_datetime(df["collection_date"])
        df.loc[:, "location"] = df["state"].map(lambda x: int(us.states.lookup(x).fips))

        # Put into long form
        out = df.melt(id_vars=["dt", "location"], value_vars=crename.keys())
        out.loc[:, "value"] = pd.to_numeric(
            out["value"].astype(str).str.replace(",", "")
        )

        # Add category, measurement, unit, age, sex, race
        out = self.extract_CMU(out, crename)
        cols_2_keep = [
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "sex",
            "race",
            "value",
        ]
        return out.loc[:, cols_2_keep]

    def normalize_icu(self, data: str) -> pd.DataFrame:
        crename = {
            "staffed adult icu beds occupied estimated": CMU(
                category="adult_icu_beds_in_use", measurement="current", unit="beds"
            ),
            "percentage of staffed adult icu beds occupied estimated": CMU(
                category="adult_icu_beds_in_use",
                measurement="current",
                unit="percentage",
            ),
            "total staffed adult icu beds": CMU(
                category="adult_icu_beds_capacity", measurement="current", unit="beds"
            ),
        }

        return self.normalize_helper(data, crename)

    def normalize_inpatient_all(self, data: str) -> pd.DataFrame:
        crename = {
            "inpatient beds occupied estimated": CMU(
                category="hospital_beds_in_use", measurement="current", unit="beds"
            ),
            "percentage of inpatient beds occupied estimated": CMU(
                category="hospital_beds_in_use",
                measurement="current",
                unit="percentage",
            ),
            "total inpatient beds": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
        }

        return self.normalize_helper(data, crename)

    def normalize_inpatient_covid(self, data: str) -> pd.DataFrame:
        crename = {
            "inpatient beds occupied by covid-19 patients estimated": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "percentage of inpatient beds occupied by covid-19 patients estimated": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="percentage",
            ),
        }

        return self.normalize_helper(data, crename)

    def normalize(self, data):
        out = pd.concat(
            [
                self.normalize_icu(data["icu"]),
                self.normalize_inpatient_all(data["inpatient_all"]),
                self.normalize_inpatient_covid(data["inpatient_covid"]),
            ],
            axis=0,
            ignore_index=True,
        )

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
            "sex",
            "value",
        ]
        return out.loc[:, cols_2_keep]
