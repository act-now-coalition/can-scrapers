import pandas as pd
import us

from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables


class TennesseeVaccineCounty(StateDashboard):
    has_location = False
    source = "https://www.tn.gov/health/cedep/ncov/covid-19-vaccine-information.html"
    source_name = "Tennessee Department of Health"
    state_fips = int(us.states.lookup("Tennessee").fips)
    location_type = "county"
    fetch_url = (
        "https://www.tn.gov/content/dam/tn/health/documents/"
        "cedep/novel-coronavirus/datasets/COVID_VACCINE_COUNTY_SUMMARY.XLSX"
    )

    variables = {
        "RECIPIENT_COUNT": variables.INITIATING_VACCINATIONS_ALL,
        "RECIP_FULLY_VACC": variables.FULLY_VACCINATED_ALL,
        "RECIP_ADDL_DOSE": variables.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
    }

    def fetch(self) -> pd.DataFrame:
        return pd.read_excel(self.fetch_url)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        odd_cap_counties = {
            "Mcnairy": "McNairy",
            "Mcminn": "McMinn",
            "Dekalb": "DeKalb",
        }
        df = self._rename_or_add_date_and_location(
            data=df,
            date_column="DATE",
            location_name_column="COUNTY",
            apply_title_case=True,
            location_names_to_drop=["OUT OF STATE"],
        ).pipe(self._reshape_variables, variable_map=self.variables)

        df["location_name"] = df["location_name"].replace(odd_cap_counties)
        return df
