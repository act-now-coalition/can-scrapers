import pandas as pd

import us
from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS

from typing import Any


class IowaCasesDeaths(ArcGIS):
    """
    Fetch county level covid data from Iowa's ARCGIS dashboard
    """

    ARCGIS_ID = "vPD5PVLI6sfkZ5E4"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Iowa").fips)
    source = "https://coronavirus.iowa.gov/pages/rmcc-data"
    service: str = "IA_COVID19_Cases"

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
        "fips"
    ]

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, "")

    def pre_normalize(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)

        # Make columns names all-lowercase
        df.columns = [x.lower() for x in list(df)]
        df = df.rename(
            columns={"name": "location_name", "last_updated": "dt"}
        )

        crename = {
            "confirmed": CMU(
                category="cases",
                measurement="cumulative",
                unit="unique_people",
            ),
            "deaths": CMU(
                category="deaths", measurement="cumulative", unit="unique_people"
            ),
            "individuals_tested": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
        }

        out = (
            df.melt(id_vars=["dt", "location_name","fips"], value_vars=crename.keys())
            .assign(dt=self._retrieve_dt("US/Central"))
            .dropna()
        )

        out = self.extract_CMU(out, crename)

        return out.loc[:, self.cols_to_keep].query("fips != '0'")


    def normalize(self, data) -> pd.DataFrame:
        # Normalize data, which is dependent on the current class
        out = self.pre_normalize(data)

        out["vintage"] = self._retrieve_vintage()
        return out


class IowaHospitals(IowaCasesDeaths):

    service: str = "COVID19_RMCC_Hospitalization"

    def pre_normalize(self, data) -> pd.DataFrame:
        df = self.arcgis_jsons_to_df(data)
