import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ArcGIS


class LAVaccineCounty(ArcGIS):
    provider = "state"
    ARCGIS_ID = "O5K6bb5dZVZcTo5M"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Louisiana").fips)
    source = "https://ladhh.maps.arcgis.com/apps/opsdashboard/index.html#/7b2a0703a129451eaaf9046c213331ed"
    source_name = "Louisiana Department of Health"

    def fetch(self):
        return self.get_all_jsons("Vaccinations_by_Race_by_Parish", 0, 5)

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df = df.rename(
            columns={
                "PFIPS": "location",
            }
        )
        df["dt"] = self._retrieve_dt("US/Central")

        melted = df.melt(
            id_vars=["dt", "location"], value_vars=["SeriesInt", "SeriesComp"]
        )

        crename = {
            "SeriesInt": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "SeriesComp": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        result = self.extract_CMU(melted, crename)

        result["vintage"] = self._retrieve_vintage()

        return result.drop(["variable"], axis="columns")
