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
    service = "Vaccine_Dashboard_Feature_Layer"
    ARCGIS_ID = "WzFsmainVTuD5KML"
    state_fips = int(us.states.lookup("Alaska").fips)

    variables = {
        "1+ Dose": variables.INITIATING_VACCINATIONS_ALL,
        "Fully Vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons(self.service, 0, "1")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)

        rename = {"Area": "location_name", "Vax_Status": "variable", "Counts": "value"}

        out = (
            df.loc[df["Age_Group"] == "All Ages"]
            .loc[:, list(rename.keys())]
            .rename(columns=rename)
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="location_name",
                timezone="US/Alaska",
            )
            .pipe(self.extract_CMU, cmu=self.variables)
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
