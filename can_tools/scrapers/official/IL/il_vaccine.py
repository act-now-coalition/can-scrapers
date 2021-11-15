import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class IllinoisVaccineCounty(StateDashboard):
    has_location = False
    source = "https://www.dph.illinois.gov/covid19/vaccinedata"
    source_name = "Illinois Department of Public Health"
    state_fips = int(us.states.lookup("Illinois").fips)
    url = "https://idph.illinois.gov/DPHPublicInformation/api/COVIDVaccine/getCOVIDVaccineAdministrationCountyAge"
    location_type = "county"

    variables = {
        # AdministeredCount appears to be the column for 1+ doses
        # and TotalAdministered is the column for total doses administered
        # AdministeredCount is always greater PersonsFullyVaccinated and less than TotalAdministered
        "AdministeredCount": variables.INITIATING_VACCINATIONS_ALL,
        "PersonsFullyVaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self) -> pd.DataFrame:
        return pd.read_json(self.url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return (
            # rename the Chicago rows such that they are summed together with the Cook County entries,
            # effectively merging the two locations into one (as CHI is within Cook County)
            data.replace({"Chicago": "Cook", "Chicago ": "Cook"})
            .groupby(["CountyName", "Report_Date"])
            .sum()
            .reset_index()
            .loc[
                :,
                [
                    "AdministeredCount",
                    "PersonsFullyVaccinated",
                    "Report_Date",
                    "CountyName",
                ],
            ]
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="CountyName",
                location_names_to_drop=["Illinois"],
                location_names_to_replace={
                    "Dekalb": "DeKalb",
                    "Dupage": "DuPage",
                    "Lasalle": "LaSalle",
                    "Mcdonough": "McDonough",
                    "Mclean": "McLean",
                    "Mchenry": "McHenry",
                },
                date_column="Report_Date",
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
        )
