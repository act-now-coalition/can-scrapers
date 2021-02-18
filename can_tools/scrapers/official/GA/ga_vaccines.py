import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ArcGIS


class GeorgiaCountyVaccine(ArcGIS):
    """
    Fetch county level covid data from an ARCGIS dashboard

    Example implementations:
      * `can_tools/scrapers/official/FL/fl_state.py`
      * `can_tools/scrapers/official/GA/ga_vaccines.py`
    """

    ARCGIS_ID = "t7DA9BjRElflTVpw"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Georgia").fips)
    source = (
        "https://experience.arcgis.com/experience/3d8eea39f5c1443db1743a4cb8948a9c/"
    )
    source_name = "Georgia Department of Public Health"

    def fetch(self):
        return self.get_all_jsons("Georgia_DPH_COVID19_Vaccination_PUBLIC_v1", 4, 6)

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [c.lower() for c in df.columns]

        # Ensure that dose_1 is "at least one dose"
        df.loc[:, "dose_1"] = df.eval("dose_1 + dose_2")

        # Fix location names and drop state data
        df.loc[:, "location_name"] = (
            df["county_name"].str.title().str.replace("County", "").str.strip()
        )
        df = df.replace(
            {
                "location_name": {
                    "Mcduffie": "McDuffie",
                    "Dekalb": "DeKalb",
                    "Mcintosh": "McIntosh",
                }
            }
        )
        df = df.query("location_name != 'Georgia'")

        crename = {
            "dose_1": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "dose_2": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "total_administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        # Extract category information and add other variable context
        out = df.melt(id_vars=["location_name"], value_vars=crename.keys())
        out = self.extract_CMU(out, crename)

        # Add date and vintage
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
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
