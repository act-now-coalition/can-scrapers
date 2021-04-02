from typing import Any

import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import ArcGIS


class NewJerseyVaccineCounty(ArcGIS):
    ARCGIS_ID = "Z0rixLlManVefxqY"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("New Jersey").fips)
    source = "https://covid19.nj.gov/#live-updates"
    source_name = "New Jersey Department of Health"
    service: str = "VaxCov2"

    variables = {
        "Dose_1": variables.INITIATING_VACCINATIONS_ALL,
        "CompletedVax": variables.FULLY_VACCINATED_ALL,
        "Grand_Total": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, 7)

    def normalize(self, data: Any) -> pd.DataFrame:
        non_counties = ["Out Of State", "Unknown", "Missing"]  # noqa
        return (
            self.arcgis_jsons_to_df(data)
            .rename(columns=dict(County="location_name"))
            .melt(
                id_vars=["location_name"],
                value_vars=list(self.variables.keys()),
            )
            .assign(
                dt=self._retrieve_dt(),
                vintage=self._retrieve_vintage(),
                location_name=lambda x: x["location_name"].str.title(),
            )
            .pipe(self.extract_ScraperVariable, cmu=self.variables)
            .drop(["variable"], axis=1)
            .query("location_name not in @non_counties")
            .dropna()
        )
