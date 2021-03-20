import pandas as pd
import us

from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables


class CaliforniaVaccineCounty(StateDashboard):
    has_location = False
    source = (
        "https://data.chhs.ca.gov/dataset/vaccine-progress-dashboard/"
        "resource/130d7ba2-b6eb-438d-a412-741bde207e1c"
    )
    source_name = "California Health & Human Services Agency"
    state_fips = int(us.states.lookup("California").fips)
    location_type = "county"

    url = (
        "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/"
        "130d7ba2-b6eb-438d-a412-741bde207e1c/download/covid19vaccinesbycounty.csv"
    )

    variables = {
        "cumulative_fully_vaccinated": variables.FULLY_VACCINATED_ALL,
        "cumulative_at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "cumulative_total_doses": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self) -> pd.DataFrame:
        return pd.read_csv(self.url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="county",
            date_column="administered_date",
            location_names_to_drop=["Outside California", "Unknown"],
        )

        data = self._reshape_variables(data, self.variables)
        return data
