import pandas as pd

import us
from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class Pennsylvania(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Pennsylvania's ARCGIS dashboard
    """

    ARCGIS_ID = "Nifc7wlHaBPig3Q3"
    has_location = True
    state_fips = int(us.states.lookup("Pennsylvania").fips)
    source = "https://experience.arcgis.com/experience/ed2def13f9b045eda9f7d22dbc9b500e"

    def fetch(self):
        return self.get_all_jsons("COVID_PA_Counties", 0, 1)

    def normalize(self, data):
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
                category="unspecified_tests_negative",
                measurement="cumulative",
                unit="test_encounters",
            ),
            "confirmed": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="test_encounters",
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

    def validate(self, df, df_hist):
        return True
