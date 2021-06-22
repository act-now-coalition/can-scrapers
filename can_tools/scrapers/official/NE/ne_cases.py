import pandas as pd
import us
import requests

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class NebraskaCases(StateDashboard):

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Nebraska").fips)
    source = "https://experience.arcgis.com/experience/ece0db09da4d4ca68252c3967aa1e9dd"
    fetch_url = (
        "https://gis.ne.gov/Enterprise/rest/services/C19Combine/FeatureServer/0/query"
    )
    source_name = "Nebraska Department of Health and Human Services"
    variables = {"attributes.PosCases": variables.CUMULATIVE_CASES_PEOPLE}

    def fetch(self) -> requests.models.Response:
        params = {
            "f": "json",
            "where": "0=0",
            "returnGeometry": "false",
            "outFields": "name,PosCases",
        }
        return requests.get(self.fetch_url, params=params)

    def normalize(self, data: requests.models.Response) -> pd.DataFrame:
        return (
            pd.json_normalize(data.json()["features"])
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="attributes.name",
                timezone="US/Central",
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
        )
