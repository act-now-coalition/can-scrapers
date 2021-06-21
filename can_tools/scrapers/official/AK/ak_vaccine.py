import pandas as pd
import us


from can_tools.scrapers.official.base import ArcGIS
from can_tools.scrapers import variables


class AlaskaCountyVaccine(ArcGIS):
    has_location = False
    location_type = "county"
    source = "https://alaska-coronavirus-vaccine-outreach-alaska-dhss.hub.arcgis.com/app/c74be37e02b44bb8b8b40515eabbab55"
    source_name = (
        "Alaska Department of Health and Social Services Coronavirus Response Hub"
    )
    service = "Vaccine_Dashboard_Map_Feature_Layer_(PRODUCTION)"
    ARCGIS_ID = "WzFsmainVTuD5KML"
    state_fips = int(us.states.lookup("Alaska").fips)

    variables = {
        "All_DoseOne_Count": variables.INITIATING_VACCINATIONS_ALL,
        "All_VaxCom_Count": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons(self.service, 0, "1")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        out = (
            df.loc[:, ["ADJ_BORO_1", "All_DoseOne_Count", "All_VaxCom_Count"]]
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="ADJ_BORO_1",
                timezone="US/Alaska",
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
            .assign(
                location_name=lambda x: x["location_name"]
                .str.replace("And", "and")
                .str.replace("Of", "of")
            )
        )
        return out.query('location_name not in ["Yakutat Plus Hoonah-Angoon", "Bristol Bay Plus Lake and Peninsula"]')

