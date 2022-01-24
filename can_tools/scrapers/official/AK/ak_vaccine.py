from numpy import var
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
    service = "Vaccine_Dashboard_Map_V2"
    ARCGIS_ID = "WzFsmainVTuD5KML"
    state_fips = int(us.states.lookup("Alaska").fips)

    variables = {
        "FDA": variables.INITIATING_VACCINATIONS_ALL,
        "SDA": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons(self.service, 0, "1")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)

        out = (
            # the data entries are repeated with different "Vax_Status values"
            # so we just choose one slice to use, as they're all the same.
            df.loc[(df["Age_Group"] == "All Ages") & (df["Vax_Status"] == "1+ Dose")]
            .loc[:, ["Name", "FDA", "SDA"]]
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="Name",
                timezone="US/Alaska",
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
            .assign(
                location_name=lambda x: x["location_name"]
                .str.replace("And", "and")
                .str.replace("Of", "of"),
                vintage=self._retrieve_vintage(),
            )
            .drop_duplicates()
        )
        return out.query(
            'location_name not in ["Yakutat Plus Hoonah-Angoon", "Bristol Bay Plus Lake and Peninsula"]'
        )
