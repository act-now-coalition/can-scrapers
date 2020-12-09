import pandas as pd

import us
from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class Wisconsin(ArcGIS, DatasetBase):
    """
    Fetch county-level covid data from Wisconsin's ARCGIS dashboard
    """

    ARCGIS_ID = ""  # not applicable on this server
    has_location = True
    location_type = "county"
    source = "https://www.dhs.wisconsin.gov/covid-19/data.htm"

    # # override query URL since Wisconsin has a self-hosted ArcGIS server with a different URL pattern
    def arcgis_query_url(
        self, service="DHS_COVID19/COVID19_WI", sheet=1, srvid="server"
    ):
        out = f"https://dhsgis.wi.gov/{srvid}/rest/services/{service}/MapServer/{sheet}/query"

        return out

    def fetch(self):
        return self.get_all_jsons("DHS_COVID19/COVID19_WI", 1, "server")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = df["geoid"].astype(int)

        crename = {
            "positive": CMU(category="cases", measurement="cumulative", unit="people"),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            "neg_new": CMU(
                category="pcr_tests_negative",
                measurement="new",
                unit="unique_people",
            ),
            "pos_new": CMU(
                category="pcr_tests_positive",
                measurement="new",
                unit="unique_people",
            ),
            "test_new": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="unique_people",
            ),
        }
        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Central"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

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
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]
