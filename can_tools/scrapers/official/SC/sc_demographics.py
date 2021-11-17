from itertools import count
from tableauscraper import TableauScraper
import us
import pandas as pd
from can_tools.scrapers import variables

from can_tools.scrapers.official.base import StateDashboard


class SCVaccineDemographics(StateDashboard):
    has_location = False
    source = "https://public.tableau.com/app/profile/population.health.dhec/viz/COVIDVaccineDashboard/RECIPIENTVIEW"
    source_name = "South Carolina Population Health DHEC"
    location_type = "county"

    state_fips = us.states.lookup("South Carolina").fips
    fetch_url = "https://public.tableau.com/views/COVIDVaccineDashboard/RECIPIENTVIEW"

    variables = {"total_vaccinations_initiated": variables.INITIATING_VACCINATIONS_ALL}

    # hard coding these because it was annoying to use getFilter on two different sheets
    races = [
        "Asian, American Indian or Alaskan Native, Native Hawaiian or Other Pacific Islander",
        "Black",
        "Other",
        "Unknown",
        "White",
    ]

    def fetch(self):
        engine = TableauScraper()
        engine.loads(self.fetch_url)

        engine = engine.getWorksheet("Vaccine Map By SC residents PEOPLE")
        filters = engine.getFilters()
        counties = [
            t["values"] for t in filters if t["column"] == "Recipient County for maps"
        ][0]

        data = []
        for county in counties:
            workbook = engine.setFilter("Recipient County for maps", county)
            for race in self.races:
                # set the filter functions to select specific race
                workbook = workbook.getWorksheet("Final Age xSex x Race REC")
                workbook = workbook.setFilter("Assigned Race", race)

                county_data = workbook.getWorksheet("Final Age xSex x Race REC").data
                data.append(county_data.assign(location_name=county))
        return pd.concat(data)

    def normalize(self, data: TableauScraper):
        rename = {
            "AGG(Count individuals with Suppression )-alias": "value",
            "Assigned Race-value": "race",
            "ATTR(recip sex (Recipient Data Vaccine Dashboard v1.csv))-alias": "sex",
            "location_name": "location_name",
            "Age Bins SIMON-value": "age",
        }
        data = (
            data.rename(columns=rename)
            .loc[:, list(rename.values())]
            .assign(
                variable="total_vaccinations_initiated",
                dt=self._retrieve_dt(),
                vintage=self._retrieve_vintage(),
                sex=lambda row: row["sex"].str.lower(),
                race=lambda row: row["race"]
                .replace(
                    "Asian, American Indian or Alaskan Native, Native Hawaiian or Other Pacific Islander",
                    "ai_an_asian_or_pacific_islander",
                )
                .str.lower(),
                age=lambda row: row["age"].str.replace("+", "_plus"),
            )
            .pipe(
                self.extract_CMU,
                cmu=self.variables,
                var_name="variable",
                skip_columns=["age", "race", "sex"],
            )
            .query("value != -1")
        )

        # sum over age and race columns to create independent demographic data
        # (e.g 12-15, all, all, all instead of 12-15, ai_an, all, all etc.)
        dataframes = []
        for variable in ["age", "race"]:
            demo_data = (
                data.groupby(
                    by=[col for col in data.columns if col not in ["value", variable]]
                )
                .sum()
                .reset_index()
            )
            demo_data[variable] = "all"
            dataframes.append(demo_data)
        return pd.concat(dataframes)
