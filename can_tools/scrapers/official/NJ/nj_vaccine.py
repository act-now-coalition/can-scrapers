from typing import Any

import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ArcGIS


class NewJerseyVaccineCounty(ArcGIS):
    ARCGIS_ID = "Z0rixLlManVefxqY"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("New Jersey").fips)
    source = "https://covid19.nj.gov/#live-updates"
    source_name = "New Jersey Department of Health"
    service: str = "VaxCov2"

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, 7)

    def normalize(self, data: Any) -> pd.DataFrame:
        cmus = {
            "Dose_1": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "Dose_2": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "Grand_Total": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }
        non_counties = ["Out Of State", "Unknown", "Missing"]  # noqa
        return (
            self.arcgis_jsons_to_df(data)
            .rename(columns=dict(County="location_name"))
            .melt(
                id_vars=["location_name"],
                value_vars=["Dose_1", "Dose_2", "Grand_Total"],
            )
            .assign(
                dt=self._retrieve_dt(),
                vintage=self._retrieve_vintage(),
                location_name=lambda x: x["location_name"].str.title(),
            )
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis=1)
            .query("location_name not in @non_counties")
        )
