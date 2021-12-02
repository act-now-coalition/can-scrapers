from itertools import count
from tableauscraper import TableauScraper
import us
import pandas as pd
from can_tools.scrapers import variables

from can_tools.scrapers.official.base import StateDashboard


class SCVaccineRace(StateDashboard):
    has_location = False
    source = "https://public.tableau.com/app/profile/population.health.dhec/viz/COVIDVaccineDashboard/RECIPIENTVIEW"
    source_name = "South Carolina Population Health DHEC"
    location_type = "county"

    state_fips = us.states.lookup("South Carolina").fips
    fetch_url = "https://public.tableau.com/views/COVIDVaccineDashboard/RECIPIENTVIEW"

    variables = {"total_vaccinations_initiated": variables.INITIATING_VACCINATIONS_ALL}
    demographic = "race"
    demographic_column = "Assigned Race For Rate-value"
    demographic_table = "at least 1 3 Age Groups x B W"

    def fetch(self):
        engine = TableauScraper()
        engine.loads(self.fetch_url)
        engine = engine.getWorkbook()

        data = []
        for county in self._retrieve_counties():
            workbook = engine.setParameter("County Parameter", county)
            county_data = workbook.getWorksheet(self.demographic_table).data
            data.append(county_data.assign(location_name=county))
        return pd.concat(data)

    def normalize(self, data: TableauScraper):
        rename = {
            "AGG(SC Residents at least 1  Vaccination with Suppression)-alias": "value",
            self.demographic_column: self.demographic,
            "location_name": "location_name",
        }
        data = (
            data.rename(columns=rename)
            .loc[:, list(rename.values())]
            .assign(
                variable="total_vaccinations_initiated",
                dt=self._retrieve_dt(),
                vintage=self._retrieve_vintage(),
                value=lambda row: pd.to_numeric(
                    row["value"].str.replace(",", "").str.replace("<5", "5")
                ),
            )
        )
        data[self.demographic] = (
            data[self.demographic]
            .str.lower()
            .str.replace("not hispanic", "non-hispanic")
        )
        data = self.extract_CMU(
            df=data,
            cmu=self.variables,
            var_name="variable",
            skip_columns=[self.demographic],
        )

        # Sum over all of the age buckets (that were implicitly dropped at the start of this method)
        return (
            data.groupby(by=[col for col in data.columns if col not in ["value"]])
            .sum()
            .reset_index()
        )


class SCVaccineEthnicity(SCVaccineRace):
    demographic = "ethnicity"
    demographic_column = "Clean Ethnicity-value"
    demographic_table = "At least 1 3 Age Groups H NH"
