from can_tools.scrapers.variables import (
    INITIATING_VACCINATIONS_ALL,
    FULLY_VACCINATED_ALL,
    PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
)
import pandas as pd

import us

from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class MarylandCountyVaccines(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Maryland's ARCGIS dashboard
    """

    ARCGIS_ID = "njFNhDsUCentVYJW"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Maryland").fips)
    source = "https://coronavirus.maryland.gov/#Vaccine"
    source_name = "Maryland Department of Health"

    variables = {
        "firstdose": INITIATING_VACCINATIONS_ALL,
        "percentfullyvaccinated": PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
        "fullyvaccinated": FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons("MD_COVID19_VaccinationByCounty", 0, "")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]

        if "singledose" in list(df):
            df["firstdose"] += df["singledose"]

        return df.pipe(
            self._rename_or_add_date_and_location,
            location_name_column="county",
            location_names_to_drop=["Unknown"],
            timezone="US/Eastern",
            apply_title_case=False,
        ).pipe(self._reshape_variables, self.variables)
