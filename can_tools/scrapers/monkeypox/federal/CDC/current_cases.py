from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard
import pandas as pd
import requests
import us


def _lookup(location):
    if location == "US":
        return 0
    return int(us.states.lookup(location).fips)


class CDCCurrentMonkeypoxCases(FederalDashboard):
    data_type = "monkeypox"
    has_location = True
    location_type = "state"
    source = "https://www.cdc.gov/poxvirus/monkeypox/response/2022/us-map.html"
    fetch_url = "https://www.cdc.gov/poxvirus/monkeypox/response/modules/MX-response-case-count-US.json"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    variables = {
        "Cases": CMU(
            category="monkeypox_cases",
            measurement="cumulative",
            unit="people",
        )
    }

    def fetch(self):
        response = requests.get(self.fetch_url)
        response.raise_for_status()
        return response.json()

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = pd.DataFrame.from_records(data["data"])
        data = data.loc[data["State"] != "Non-US Resident"].assign(
            location=lambda row: row["State"].map(_lookup), dt=self._retrieve_dt()
        )
        return self._reshape_variables(data=data, variable_map=self.variables)
