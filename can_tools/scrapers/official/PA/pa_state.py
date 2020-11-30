import pandas as pd

import us
from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS

from typing import Dict

class Pennsylvania(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Pennsylvania's ARCGIS dashboard
    """

    ARCGIS_ID = "Nifc7wlHaBPig3Q3"
    has_location = True
    state_fips = int(us.states.lookup("Pennsylvania").fips)
    source = "https://experience.arcgis.com/experience/ed2def13f9b045eda9f7d22dbc9b500e"

    def fetch(self) -> Dict:
        raw_data = {}
        raw_data["cases_deaths"] = self.get_all_jsons("COVID_PA_Counties", 0, 1)
        raw_data["hospitals"] = self.get_all_jsons("covid_hosp_single_day", 0, 1)
        return raw_data

    def normalize(self, data) -> pd.DataFrame:
        cases_deaths = self.normalize_cases_deaths(data["cases_deaths"])
        hospitals = self.normalize_hospitals(data["hospitals"])

        out = pd.concat([cases_deaths, hospitals], axis=0, ignore_index=True, sort=True)
        return out

    def normalize_cases_deaths(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]

        crename = {
            "cases": CMU(category="cases", measurement="cumulative", unit="people"),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            "probable": CMU(
                category="cases_probable",
                measurement="cumulative",
                unit="people",
            ),
            "negative": CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            "confirmed": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
        }
        out = (
            df.melt(id_vars=["county"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "county",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]

    def normalize_hospitals(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]

        crename = {
            "med_total": CMU(category="hospital_beds_capacity", measurement="current", unit="beds"),
            "covid_patients": CMU(category="hospital_beds_in_use_covid", measurement="current", unit="beds"),
            "icu_total": CMU(category="icu_beds_capacity", measurement="current", unit="beds"),
            "icu_avail": CMU(category="icu_beds_available", measurement="current", unit="beds")
        }

        out = (
            df.melt(id_vars=["county"], value_vars=crename.keys())
            .assign(
                dt = self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        out = self.extract_CMU(out,crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "county",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]
        return out.loc[:, cols_to_keep]

    def validate(self, df, df_hist):
        return True
