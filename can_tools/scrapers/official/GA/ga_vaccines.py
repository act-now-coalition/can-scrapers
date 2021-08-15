import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class GeorgiaCountyVaccine(StateDashboard):

    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Georgia").fips)
    source_name = "Georgia Department of Public Health"
    source = (
        "https://experience.arcgis.com/experience/3d8eea39f5c1443db1743a4cb8948a9c/"
    )
    fetch_url = "https://georgiadph.maps.arcgis.com/sharing/rest/content/items/e7378d64d3fa4bc2a67b2ea40e4748b0/data"

    variables = {
        "CUMPERSONCVAX": variables.FULLY_VACCINATED_ALL,
        "CUMPERSONVAX": variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self) -> requests.models.Response:
        return requests.get(self.fetch_url)

    def normalize(self, data: requests.models.Response) -> pd.DataFrame:
        initiated_sheet = pd.read_excel(
            data.content, sheet_name="PERSON_1_VAX_BY_DAY_COUNTY"
        )
        completed_sheet = pd.read_excel(
            data.content, sheet_name="PERSON_C_VAX_BY_DAY_COUNTY"
        )

        # doses are stored in separate sheets, parse both
        dataframes = []
        for sheet in (initiated_sheet, completed_sheet):
            dataframes.append(
                self._rename_or_add_date_and_location(
                    data=sheet,
                    location_column="COUNTY_ID",
                    # Remove unwanted fips codes
                    # 0 = Georgia
                    # 99999 = Unknown
                    locations_to_drop=[0, 99999],
                    date_column="ADMIN_DATE",
                )
            )

        # unpack dataframes and merge into one df on location and date
        initiated, completed = dataframes
        data = pd.merge(initiated, completed, how="left", on=["location", "dt"])
        return self._reshape_variables(data=data, variable_map=self.variables)


class GeorgiaCountyVaccineAge(GeorgiaCountyVaccine):
    demographic = "age"
    sheet_name = "AGE_BY_COUNTY"
    location_column = "COUNTYFIPS"
    has_location = True
    variables = {"PERSONVAX": variables.INITIATING_VACCINATIONS_ALL}
    demographic_formatting = {
        "00-05": "0-5",
        "05_09": "5-9",
        "10_14": "10-14",
        "15_19": "15-19",
        "20_24": "20-24",
        "25_34": "25-34",
        "35_44": "35-44",
        "45_54": "45-54",
        "55_64": "55-64",
        "65_74": "65-74",
        "75_84": "75-84",
        "85PLUS": "85_plus",
    }

    def normalize(self, data: requests.models.Response) -> pd.DataFrame:
        sheet = pd.read_excel(data.content, sheet_name=self.sheet_name)
        data = self._rename_or_add_date_and_location(
            data=sheet,
            location_column=self.location_column,
            # Remove unwanted fips codes
            # 0 = Georgia
            # 99999 = Unknown
            locations_to_drop=[0, 99999],
            timezone="US/Eastern",
        )
        return (
            data.pipe(
                self._reshape_variables,
                variable_map=self.variables,
                id_vars=[self.demographic.upper()],
                skip_columns=[self.demographic],
            )
            # format the demographic column name, and standardize the values within
            .rename(columns={self.demographic.upper(): self.demographic}).replace(
                self.demographic_formatting
            )
        )


class GeorgiaCountyVaccineRace(GeorgiaCountyVaccineAge):
    demographic = "race"
    sheet_name = "RACE_BY_COUNTY"
    location_column = "COUNTY_ID"
    demographic_formatting = {
        "American Indian or Alaska Native": "ai_an",
        "Asian": "asian",
        "Black": "black",
        "White": "white",
        "Other": "other",
        "Unknown": "unknown",
    }


class GeorgiaCountyVaccineSex(GeorgiaCountyVaccineAge):
    demographic = "sex"
    sheet_name = "SEX_BY_COUNTY"
    demographic_formatting = {"Male": "male", "Female": "female", "Unknown": "unknown"}


class GeorgiaCountyVaccineEthnicity(GeorgiaCountyVaccineAge):
    demographic = "ETHNICTY"
    sheet_name = "ETHNICITY_BY_COUNTY"
    demographic_formatting = {"Hispanic": "hispanic", "Non-Hispanic": "non-hispanic"}

    def normalize(self, data: requests.models.Response) -> pd.DataFrame:
        data = super().normalize(data)
        # manually drop/rename column to fix different spelling
        return data.drop(columns={"ethnicity"}).rename(
            columns={"ETHNICTY": "ethnicity"}
        )
