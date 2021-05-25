from typing import Any

import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS


class NewJerseyVaccineCounty(ArcGIS):
    ARCGIS_ID = "Z0rixLlManVefxqY"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("New Jersey").fips)
    source = "https://covid19.nj.gov/#live-updates"
    source_name = "New Jersey Department of Health"
    service: str = "VaxCov2"

    # NOTE: do not delete the `(start|end) variables` comments
    #       they are needed to generate documentation
    # start variables
    variables = {
        "Dose_1": variables.INITIATING_VACCINATIONS_ALL,
        "CompletedVax": variables.FULLY_VACCINATED_ALL,
        "Grand_Total": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }
    # end variables

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, 7)

    def normalize(self, data: Any) -> pd.DataFrame:
        non_counties = ["OUT OF STATE", "UNKNOWN", "MISSING", "TOTALS"]
        df = self.arcgis_jsons_to_df(data)
        df = self._rename_or_add_date_and_location(
            df,
            location_name_column="County",
            timezone="US/Eastern",
            location_names_to_drop=non_counties,
        )
        return self._reshape_variables(df, self.variables)
