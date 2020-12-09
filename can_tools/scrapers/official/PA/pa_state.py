import pandas as pd

import us
from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS

from typing import Any


class PennsylvaniaCasesDeaths(ArcGIS):
    """
    Fetch county level covid data from Pennsylvania's ARCGIS dashboard
    """

    ARCGIS_ID = "Nifc7wlHaBPig3Q3"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Pennsylvania").fips)
    source = "https://experience.arcgis.com/experience/ed2def13f9b045eda9f7d22dbc9b500e"
    service: str = "COVID_PA_Counties"

    cols_to_keep = [
        "dt",
        "location_name",
        "category",
        "measurement",
        "unit",
        "age",
        "race",
        "sex",
        "value",
    ]

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, 1)

    def pre_normalize(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)

        # Make columns names all-lowercase
        df.columns = [x.lower() for x in list(df)]
        df = df.rename(columns={"county": "location_name"})

        crename = {
            "cases": CMU(category="cases", measurement="cumulative", unit="people"),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            # "probable": CMU(
            #     category="cases_probable",
            #     measurement="cumulative",
            #     unit="people",
            # ),
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
            df.melt(id_vars=["location_name"], value_vars=crename.keys())
            .assign(dt=self._retrieve_dt("US/Eastern"))
            .dropna()
            .replace(dict(location_name=dict(Mckean="McKean")))
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        return out.loc[:, self.cols_to_keep].query("location_name != 'Pennsylvania'")

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
        df = df.rename(columns={"county": "location_name"})

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

        out = df.melt(
            id_vars=["location_name", "dt"], value_vars=crename.keys()
        ).dropna()

        non_county_regions = [
            "Pennsylvania",
            "East Central HCC",
            "HCC of Southwest PA",
            "Keystone HCC",
            "North Central HCC",
            "Northeast",
            "Northcentral",
            "Northeast HCC",
            "Northern Tier HCC",
            "Northwest",
            "Southcentral",
            "Southeast HCC",
            "Southeast",
            "Southwest",
        ]

        out = out[~out["location_name"].isin(non_county_regions)]

        out.loc[:, "value"] = pd.to_numeric(out["value"])

        out = self.extract_CMU(out, crename)

        return out.loc[:, self.cols_to_keep]
