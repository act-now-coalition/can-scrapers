import pandas as pd

import us
from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS

from typing import Any


class PennsylvaniaCasesDeaths(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Pennsylvania's ARCGIS dashboard
    """

    ARCGIS_ID = "Nifc7wlHaBPig3Q3"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Pennsylvania").fips)
    source = "https://experience.arcgis.com/experience/ed2def13f9b045eda9f7d22dbc9b500e"
    service: str = "COVID_PA_Counties"

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, 1)

    def pre_normalize(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)

        # Make columns names all-lowercase
        df.columns = [x.lower() for x in list(df)]

        crename = {
            "cases": CMU(
                category="cases", measurement="cumulative", unit="unique_people"
            ),
            "deaths": CMU(
                category="deaths", measurement="cumulative", unit="unique_people"
            ),
            "probable": CMU(
                category="cases_probable",
                measurement="cumulative",
                unit="unique_people",
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
            .assign(dt=self._retrieve_dt("US/Eastern"))
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
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

    def normalize(self, data) -> pd.DataFrame:
        # Normalize data, which is dependent on the current class
        out = self.pre_normalize(data)

        out["vintage"] = self._retrieve_vintage()
        return out


class PennsylvaniaHospitals(PennsylvaniaCasesDeaths):

    service: str = "covid_hosp"

    def pre_normalize(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)

        # Make columns names all-lowercase
        df.columns = [x.lower() for x in list(df)]

        crename = {
            "med_total": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
            "covid_patients": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "icu_total": CMU(
                category="icu_beds_capacity", measurement="current", unit="beds"
            ),
            "icu_avail": CMU(
                category="icu_beds_available", measurement="current", unit="beds"
            ),
        }

        df["dt"] = df["date"].map(self._esri_ts_to_dt)

        out = df.melt(id_vars=["county", "dt"], value_vars=crename.keys()).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        out = self.extract_CMU(out, crename)

        cols_to_keep = [
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
