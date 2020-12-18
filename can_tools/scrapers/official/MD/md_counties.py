import pandas as pd

import us
from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class Maryland(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Maryland's ARCGIS dashboard
    """

    ARCGIS_ID = "njFNhDsUCentVYJW"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Maryland").fips)
    source = "https://coronavirus.maryland.gov/"

    def fetch(self):
        return self.get_all_jsons(
            "MDH_COVID_19_Dashboard_Feature_Layer_Counties_MEMA", 0, ""
        )

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        print(df)
        df["location"] = (self.state_fips * 1000) + df["county_fip"].astype(int)

        crename = {
            "totalcasecount": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "totaldeathcount": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "total_pop_tested": CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            "total_testing_vol": CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "daily_testing_vol": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="specimens",
            ),
        }
        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
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
