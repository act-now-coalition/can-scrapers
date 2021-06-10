import us
import requests
import pandas as pd
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import CountyDashboard


class ArizonaPimaVaccine(CountyDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)
    source = "https://pimamaps.maps.arcgis.com/apps/dashboards/05c0a87f6f514b3a9220ea7fd25486a4"
    source_name = "Pima County Health Department"
    url = "https://services2.arcgis.com/UTBp78iglGpbqp1B/arcgis/rest/services/VaccZip/FeatureServer/0/query"
    variables = {
        "Sum_FIRST": variables.INITIATING_VACCINATIONS_ALL,
        "Sum_COMPLE": variables.FULLY_VACCINATED_ALL,
    }
    params = {
        "f": "json",
        "outFields": "*",
        "outStatistics": "",
        "resultType": "standard",
        "returnGeometry": "false",
    }

    def _get_date(self):
        self.params[
            "outStatistics"
        ] = '[{"onStatisticField":"ADMIN_DATE","outStatisticFieldName":"maxdate","statisticType":"max"}]'
        dt_url = "https://services2.arcgis.com/UTBp78iglGpbqp1B/arcgis/rest/services/VaccData/FeatureServer/0/query"
        res = requests.get(dt_url, params=self.params).json()
        dt = res["features"][0]["attributes"]["maxdate"]
        return pd.to_datetime(dt, unit="ms").date()

    def fetch(self):
        self.params[
            "outStatistics"
        ] = '[{"onStatisticField":"Sum_FIRST","outStatisticFieldName":"Sum_FIRST","statisticType":"sum"},{"onStatisticField":"Sum_COMPLE","outStatisticFieldName":"Sum_COMPLE","statisticType":"sum"}]'
        return requests.get(self.url, params=self.params)

    def normalize(self, data):
        rows = data.json()["features"][0]["attributes"]
        return (
            pd.DataFrame(
                rows,
                index=[0],
            )
            .assign(location_name="Pima", dt=self._get_date())
            .pipe(self._reshape_variables, variable_map=self.variables)
        )
