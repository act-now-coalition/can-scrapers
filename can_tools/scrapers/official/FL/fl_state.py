import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS


class Florida(ArcGIS):
    """
    Fetch county level covid data from Florida's ARCGIS dashboard
    """

    ARCGIS_ID = "CY1LXxl9zlJeBuRZ"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Florida").fips)
    source = "https://experience.arcgis.com/experience/96dd742462124fa0b38ddedb9b25e429"
    source_name = "Florida Department of Health"

    def fetch(self):
        return self.get_all_jsons("Florida_COVID19_Cases", 0, 1)

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = (self.state_fips * 1000) + df["county"].astype(int)

        # 12025 is the OLD (retired in 1997) fips code for Date county. It is now known
        # as Miami-Dade county with fips code 12086
        df.loc[:, "location"] = df["location"].replace(12025, 12086)

        crename = {
            "casesall": CMU(category="cases", measurement="cumulative", unit="people"),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            "newpos": CMU(
                category="unspecified_tests_positive",
                measurement="new",
                unit="test_encounters",
            ),
            "newneg": CMU(
                category="unspecified_tests_negative",
                measurement="new",
                unit="test_encounters",
            ),
            "newtested": CMU(
                category="unspecified_tests_total",
                measurement="new",
                unit="test_encounters",
            ),
        }
        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .query("location not in (12998, 12999)")
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
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]
