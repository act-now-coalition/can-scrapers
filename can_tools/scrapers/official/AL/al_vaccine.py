import pandas as pd
import us

from can_tools.scrapers import CMU, variables
from can_tools.scrapers.official.base import ArcGIS

pd.options.mode.chained_assignment = None  # Avoid unnessacary SettingWithCopy warning


class ALCountyVaccine(ArcGIS):
    ARCGIS_ID = "4RQmZZ0yaZkGR1zy"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Alabama").fips)
    source = "https://alpublichealth.maps.arcgis.com/apps/opsdashboard/index.html#/e4a232feb1344ce0afd9ac162f3ac4ba"
    source_name = "Alabama Department of Public Health"

    variables = {
        "PERSONVAX": variables.INITIATING_VACCINATIONS_ALL,
        "PERSONCVAX": variables.FULLY_VACCINATED_ALL,
        "NADMIN": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        service = "Vax_Dashboard_Public_3_VIEW"
        return self.get_all_jsons(service, 1, "7")

    def normalize(self, data):
        data = self.arcgis_jsons_to_df(data)
        data = self._rename_or_add_date_and_location(
            data, location_column="CNTYFIPS", timezone="US/Central"
        )
        data = self._reshape_variables(data, self.variables)
        locations_to_drop = [0, 99999]
        data = data.query("location != @locations_to_drop")
        return data
