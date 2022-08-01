from tableauscraper import TableauScraper
import us
import pandas as pd
from can_tools.scrapers import variables

from can_tools.scrapers.official.base import StateDashboard


class ArizonaVaccineRace(StateDashboard):
    has_location = False
    source = "https://www.azdhs.gov/covid19/data/index.php#vaccine-admin"
    source_name = "ARIZONA DEPARTMENT OF HEALTH SERVICES"
    location_type = "county"
    timezone = "US/Mountain"
    demographic_data = True

    state_fips = us.states.lookup("Arizona").fips

    variables = {"total_vaccinations_initiated": variables.INITIATING_VACCINATIONS_ALL}
    fetch_url = (
        "https://tableau.azdhs.gov/views/VaccineDashboard/Vaccineadministrationdata"
    )

    def fetch(self):
        engine = TableauScraper()
        engine.loads(self.fetch_url)
        return engine.getWorksheet(" County using admin county")

    def normalize(self, worksheet: TableauScraper) -> pd.DataFrame:

        # extract data from Tableau Dashboard
        counties = [
            t["values"]
            for t in worksheet.getSelectableItems()
            if t["column"] == "Admin Address County W/ State Pod"
        ][0]
        raw_data = []
        for county in counties:
            workbook = worksheet.select("Admin Address County W/ State Pod", county)
            raw_data.append(
                workbook.getWorksheet("Race (# people)").data.assign(
                    location_name=county
                )
            )
        data = pd.concat(raw_data)

        # format data to match structure
        rename_columns = {
            "Race/Ethnicity Consolidated (group)-alias": "race",
            "AGG(Total number of people (LOD))-alias": "value",
        }
        rename_demos = {
            "Asian or Pacific Islander, non-Hispanic": "asian_or_pacific_islander",
            "Black or African American, non-Hispanic": "black",
            "American Indian or Alaska Native, non-Hispanic": "ai_an",
            "Other Race": "other",
            "White, non-Hispanic": "white",
            "Unknown": "unknown",
            "Hispanic or Latino": "all",
        }

        # All the race demographics specify that the entries are non-hispanic
        # except for 'Hispanic or Latino' value.
        # So, by default set ethnicity as non-hispanic, and set to hispanic where race == Hispanic or Latino
        # When race is unknown, ethnicity is also unknown, modify columns to reflect this.
        data = (
            data.loc[:, ["location_name"] + list(rename_columns.keys())]
            .rename(columns=rename_columns)
            .assign(
                race=lambda row: row["race"].replace(rename_demos),
                ethnicity="non-hispanic",
                variable="total_vaccinations_initiated",
                dt=self._retrieve_dt(),
                vintage=self._retrieve_vintage(),
            )
            .pipe(
                self.extract_CMU, cmu=self.variables, skip_columns=["race", "ethnicity"]
            )
        )

        data.loc[data["race"] == "all", "ethnicity"] = "hispanic"
        data.loc[data["race"] == "unknown", "ethnicity"] = "unknown"
        return data
