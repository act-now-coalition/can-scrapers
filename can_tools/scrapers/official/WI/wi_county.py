import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.WI.common import WisconsinArcGIS


class WisconsinCounties(WisconsinArcGIS):
    """
    Fetch county-level covid data from Wisconsin's ARCGIS dashboard
    """

    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Wisconsin").fips)
    source = "https://www.dhs.wisconsin.gov/covid-19/data.htm"

    def fetch(self):
        return self.get_all_jsons("DHS_COVID19/COVID19_WI", 1, "server")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = df["geoid"].astype(int)

        crename = {
            "positive": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            "negative": CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            "pos_new": CMU(
                category="pcr_tests_positive",
                measurement="new",
                unit="unique_people",
            ),
            "neg_new": CMU(
                category="pcr_tests_negative",
                measurement="new",
                unit="unique_people",
            ),
            "test_new": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="unique_people",
            ),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            "dth_new": CMU(category="deaths", measurement="new", unit="people"),
            "hosp_yes": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
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
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]
