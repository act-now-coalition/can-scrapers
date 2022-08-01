import pandas as pd
import us
import os
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS
from can_tools.scrapers import variables


class WisconsinVaccineCountyRace(TableauDashboard):

    has_location = False
    source = "https://www.dhs.wisconsin.gov/covid-19/vaccine-data.htm#summary"
    source_name = "Wisconsin Department of Health Services"
    state_fips = int(us.states.lookup("Wisconsin").fips)
    demographic_data = True
    timezone = "US/Central"

    demographic_worksheet = "Race vax/unvax county"
    demographic_column = "Race-alias"
    location_type = "county"
    demographic = "race"
    fullUrl = "https://bi.wisconsin.gov/t/DHS/views/VaccinesAdministeredtoWIResidents/VaccinatedWisconsin-County"

    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "completed": variables.FULLY_VACCINATED_ALL,
    }

    def _get_demographic_data(self, category: str, demo_col_name: str):
        # open connection to dashboard
        ts = TS()
        ts.loads(self.fullUrl)

        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            )
            .query(f"state == 55 and location != 55")["name"]
            .replace(
                {
                    "La Crosse": "La crosse",
                    "Green Lake": "Green lake",
                    "Fond du Lac": "Fond du lac",
                    "St. Croix": "St croix",
                    "Eau Claire": "Eau claire",
                }
            )
        )

        # get worksheets and set filter type
        # returns initiated values by default -- specify if the filter needs to be changed to completed
        workbook = ts.getWorkbook()
        if category == "completed":
            workbook.setParameter(
                "Initiation or Completion",
                "Total population who have completed the series",
            )
        elif category not in ["completed", "initiated"]:
            raise ValueError(
                "category expected 'completed' or 'iniated' but received neither"
            )

        # get main sheet
        ws = workbook.getWorksheet("Map")
        dfs = []
        # for each county, extract data from subsheet and rename columns, append to list of dfs
        for c in counties:
            county_ws = ws.select("County", f"{c} County")
            df = county_ws.getWorksheet(self.demographic_worksheet).data
            df = df[
                [
                    demo_col_name,
                    "AGG(Geography TT (copy))-alias",
                    "SUM(Initiation or completed count for TT (copy))-alias",
                ]
            ]
            df = df.rename(
                columns={
                    "SUM(Initiation or completed count for TT (copy))-alias": category
                }
            )
            dfs.append(df)
        return pd.concat(dfs)

    def fetch(self):
        # get initiated and completed tables then concatenate
        init = self._get_demographic_data("initiated", self.demographic_column)
        complete = self._get_demographic_data("completed", self.demographic_column)
        return pd.merge(init, complete, how="left")

    def normalize(self, data):
        data = data.rename(columns={"AGG(Geography TT (copy))-alias": "location_name"})
        data["location_name"] = (
            data["location_name"]
            .str.title()
            .str.replace(" County", "")
            .replace({"St Croix": "St. Croix", "Fond Du Lac": "Fond du Lac"})
        )
        data[self.demographic_column] = (
            data[self.demographic_column]
            .str.lower()
            .replace({"american indian": "ai_an"})
        )
        out = (
            data.melt(
                id_vars=["location_name", self.demographic_column],
                value_vars=self.variables.keys(),
            )
            .dropna()
            .assign(
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(["variable"], axis=1)
        )
        out["dt"] = self._retrieve_dt(self.timezone)
        out["vintage"] = self._retrieve_vintage()

        return out.drop(columns={self.demographic}).rename(
            columns={self.demographic_column: self.demographic}
        )


class WisconsinVaccineCountySex(WisconsinVaccineCountyRace):
    demographic_worksheet = "Sex vax/unvax county"
    demographic_column = "Sex-alias"
    demographic = "sex"


class WisconsinVaccineCountyAge(WisconsinVaccineCountyRace):
    demographic_worksheet = "Age vax/unvax County"
    demographic_column = "Age-alias"
    demographic = "age"

    def normalize(self, data):
        df = super().normalize(data)
        return df.replace("65+", "65_plus")


class WisconsinVaccineCountyEthnicity(WisconsinVaccineCountyRace):
    demographic_worksheet = "Ethnicity vax/unvax county"
    demographic_column = "Ethnicity-alias"
    demographic = "ethnicity"
