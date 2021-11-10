from can_tools.scrapers.official.base import StateDashboard
import us
import pandas as pd
from can_tools.scrapers import variables

class MIVaccineRaceAge(StateDashboard):
    has_location = False
    source = "https://www.michigan.gov/coronavirus/0,9753,7-406-98178_103214_103272-547150--,00.html"
    source_name = "State of Michican Official Website"
    state_fips = int(us.states.lookup("Michigan").fips)
    url = "https://www.michigan.gov/documents/coronavirus/Ethnicity-Race_Coverage_by_County-20210820_733398_7.xlsx"
    location_type = "county"

    variables = {
        "Initiation": variables.INITIATING_VACCINATIONS_ALL,
        "Completion": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return pd.read_excel(self.url, sheet_name="COVID Vaccine Coverage")

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = (   
            self._rename_or_add_date_and_location(
                data=data,
                location_name_column="County",
                timezone="US/Central",
                location_names_to_drop=["No County"]
            )
            .rename(columns={"Dose": "variable", "Residents Vaccinated": "value"})
            .assign(
                vintage=self._retrieve_vintage(),
                age=lambda row: row["Age Group"].str.replace(" years", "").str.replace("+", "_plus").str.replace("missing", "unknown"),
                race=lambda row: row["Race/Ethnicity"].str.replace("NH ", "").str.lower()
            )
            .loc[:,["location_name", "variable", "value", "age", "race", "dt", "vintage",]]
            .pipe(self.extract_CMU, cmu=self.variables, skip_columns=["age", "race"])
        )
        data["race"] = data["race"].replace({
            "american indian/alaska native": "ai_an",
            "asian/native hawaiian/other pacific islands": "asian_or_pacific_islander",
            "unknown, other or suppressed": "unknown",
        })

        # combine detroit and wayne county into one location
        data["location_name"] = data["location_name"].replace("Detroit", "Wayne")
        data = data.groupby(by=[col for col in data.columns if col != "value"]).sum().reset_index()
        return data