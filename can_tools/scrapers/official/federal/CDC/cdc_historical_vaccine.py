import pandas as pd
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import FederalDashboard


class CDCCountyVaccine2(FederalDashboard):
    has_location = True
    location_type = "county"
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc2"

    variables = {
        "Administered_Dose1_Recip": variables.INITIATING_VACCINATIONS_ALL,
        "Series_Complete_Yes": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return pd.read_csv(
            "https://data.cdc.gov/api/views/8xkx-amqh/rows.csv?accessType=DOWNLOAD"
        )

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        out = self._rename_or_add_date_and_location(
            data, location_column="FIPS", date_column="Date", locations_to_drop=["UNK"]
        )
        out = self._reshape_variables(out, self.variables)

        # remove entries with 0's and a location that caused issues (St. John Island, VI)
        return out.query("location != 78020 and value != 0")
