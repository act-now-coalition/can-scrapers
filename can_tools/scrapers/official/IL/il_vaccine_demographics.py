import requests
import us
import pandas as pd
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class ILVaccineRace(StateDashboard):
    has_location = False
    source = "https://dph.illinois.gov/covid19/vaccine/vaccine-data"
    source_name = "Illinois Department of Public Health"
    state_fips = int(us.states.lookup("Illinois").fips)
    url = "https://idph.illinois.gov/DPHPublicInformation/api/covidvaccine/getVaccineAdministrationDemos?countyname={county}"
    location_type = "county"
    demographic_data = True

    variables = {
        "PersonsVaccinatedOneDose": variables.INITIATING_VACCINATIONS_ALL,
        "PersonsFullyVaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        counties = self._retrieve_counties()
        county_jsons = [
            requests.get(self.url.format(county=county)).json() for county in counties
        ]
        return county_jsons

    def normalize(self, county_jsons) -> pd.DataFrame:
        # TODO(michael): There's also 'Gender' in here data if we want it.
        data = pd.concat([pd.DataFrame(json["Race"]) for json in county_jsons])

        data = self._rename_or_add_date_and_location(
            data=data,
            date_column="Report_Date",
            location_name_column="CountyName",
            location_names_to_replace={
                "Dekalb": "DeKalb",
                "Dupage": "DuPage",
                "Lasalle": "LaSalle",
                "Mcdonough": "McDonough",
                "Mclean": "McLean",
                "Mchenry": "McHenry",
            },
            location_names_to_drop=[],
        ).assign(
            vintage=self._retrieve_vintage(),
            race=data["Race"]
            .str.replace(" ", "_")
            .str.lower()
            .replace(
                {
                    "black_or_african-american": "black",
                    "american_indian_or_alaska_nati": "ai_an",
                    "native_hawaiian_or_other_pacif": "pacific_islander",
                    "hispanic_or_latino": "hispanic",
                    "other_race": "other",
                }
            ),
        )
        data = self._reshape_variables(
            data, variable_map=self.variables, id_vars=["race"], skip_columns=["race"]
        )

        # shift hispanic values to ethnicity column
        data.loc[data["race"] == "hispanic", "ethnicity"] = "hispanic"
        data.loc[data["ethnicity"] == "hispanic", "race"] = "all"

        return data
