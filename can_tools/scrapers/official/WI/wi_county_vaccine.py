from can_tools.models import Location
from typing import Dict
import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS


class WisconsinVaccineCounty(ArcGIS):
    has_location = True
    source = "https://www.dhs.wisconsin.gov/covid-19/vaccine-data.htm#summary"
    source_name = "Wisconsin Department of Health Services"
    state_fips = int(us.states.lookup("Wisconsin").fips)
    location_type = "county"

    variables = {
        "DOSE_ONE_TOTAL": variables.INITIATING_VACCINATIONS_ALL,
        "DOSE_COMPLETE_TOTAL": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self) -> Dict:
        url = "https://dhsgis.wi.gov/server/rest/services/DHS_COVID19/COVID19_WI_Vaccinations/MapServer/1/query"
        return requests.get(url, params=self.params).json()

    def normalize(self, data: Dict) -> pd.DataFrame:
        data = self.arcgis_json_to_df(data)
        data["dt"] = pd.to_datetime(data["DATE"], unit="ms").dt.date

        return data.pipe(
            self._rename_or_add_date_and_location,
            location_column="GEOID",
            date_column="dt",
        ).pipe(self._reshape_variables, variable_map=self.variables)
