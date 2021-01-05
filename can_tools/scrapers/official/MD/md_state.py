import pandas as pd

import us
from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class MarylandState(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Maryland's ARCGIS dashboard, roll up to state level.
    """

    ARCGIS_ID = "njFNhDsUCentVYJW"
    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Maryland").fips)
    source = "https://coronavirus.maryland.gov/"

    def fetch(self):
        return self.get_all_jsons("MASTERCaseTracker", 0, "")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = self.state_fips
        df = df.tail(1).reset_index(drop=True)
        df = df[
            [
                "location",
                "totalcases",
                "casedelta",
                "negativetests",
                "negdelta",
                "bedstotal",
                "bedsicu",
                "bedsdelta",
                "deaths",
                "deathsdelta",
                "totaltests",
                "testsdelta",
                "postestpercent",
            ]
        ]
        crename = {
            "totalcases": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "casedelta": CMU(category="cases", measurement="new", unit="people"),
            "negativetests": CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            "negdelta": CMU(
                category="pcr_tests_negative", measurement="new", unit="specimens"
            ),
            "bedstotal": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "bedsicu": CMU(
                category="icu_beds_in_use_covid", measurement="current", unit="beds"
            ),
            "bedsdelta": CMU(
                category="hospital_beds_in_use_covid", measurement="new", unit="beds"
            ),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            "deathsdelta": CMU(category="deaths", measurement="new", unit="people"),
            "totaltests": CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "testsdelta": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="specimens",
            ),
            "postestpercent": CMU(
                category="pcr_tests_positive",
                measurement="rolling_average_7_day",
                unit="percentage",
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
