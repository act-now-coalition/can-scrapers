from typing import Dict
from can_tools.scrapers.official.base import TableauDashboard
import us
from can_tools.scrapers import variables
import pandas as pd


class OregonVaccineRace(TableauDashboard):
    has_location = False
    source = "https://covidvaccine.oregon.gov/"
    source_name = "Oregon Health Authority"
    state_fips = int(us.states.lookup("Oregon").fips)
    location_type = "county"

    baseurl = "https://public.tableau.com/"
    viewPath = "OregonCOVID-19VaccinationTrends/OregonCountyVaccinationTrends"
    filterFunctionName = "[federated.0t5ugmz0hnw7q719jeh0615iizas].[none:county:nk]"

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
    }
    demographic = "race"

    column_renames = {
        "Demographic Category-value": "race",
        "AGG(Race Suppression)-alias": "value",
    }

    demographic_rename = {
        "Unknown": "unknown",
        "Other Race": "other",
        "NH/PI": "pacific_islander",
        "AI/AN": "ai_an",
    }

    data_tableau_table = "Cty Race"
    timezone = "US/Pacific"

    def fetch(self):
        counties = self._retrieve_counties()
        results = []
        for county in counties:
            self.filterFunctionValue = county
            results.append(
                self.get_tableau_view()[self.data_tableau_table].assign(
                    location_name=county
                )
            )
        return results

    def normalize(self, data: Dict) -> pd.DataFrame:
        data = (
            pd.concat(data)
            .rename(columns=self.column_renames)
            .query("value != 'Suppressed'")
            .assign(
                variable="total_vaccine_initiated",
                value=lambda row: pd.to_numeric(
                    row["value"].astype(str).str.replace(",", "")
                ),
                vintage=self._retrieve_vintage(),
            )
            .loc[:, ["location_name", self.demographic, "value", "variable", "vintage"]]
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="location_name",
                timezone=self.timezone,
            )
            .pipe(
                self.extract_CMU,
                cmu=self.variables,
                var_name="variable",
                skip_columns=[self.demographic],
            )
        )
        data[self.demographic] = (
            data[self.demographic].replace(self.demographic_rename).str.lower()
        )
        return data


class OregonVaccineAge(OregonVaccineRace):
    data_tableau_table = "Cty Age "
    demographic = "age"

    column_renames = {
        "Demographic Category-value": "age",
        "SUM(People Count)-alias": "value",
    }

    # this isn't needed but is used in the base class, so I'm going to leave it for the sake of simplicity
    demographic_rename = {}

    def normalize(self, data: Dict) -> pd.DataFrame:
        data = super().normalize(data)
        data["age"] = (
            data["age"]
            .str.replace(" ", "")
            .str.replace("to", "-")
            .str.replace("+", "_plus")
        )
        return data


class OregonVaccineSex(OregonVaccineRace):
    data_tableau_table = "Cty Sex"
    demographic = "sex"

    column_renames = {
        "Demographic Category-value": "sex",
        "SUM(People Count)-alias": "value",
    }

    demographic_rename = {"U": "unknown", "F": "female", "M": "male"}
