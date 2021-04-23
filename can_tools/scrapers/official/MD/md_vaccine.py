import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS


class MarylandCountyVaccines(ArcGIS):
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
        "firstdose": variables.INITIATING_VACCINATIONS_ALL,
        "fullyvaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons("MD_COVID19_VaccinationByCounty", 0, "")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]

        # singledose was added only after J&J vaccines started.
        # If it appears it will only be reported in the fully vacinated
        # column. We also wanted to include it in `firstdose` b/c we
        # report "people with at least one dose"
        if "singledose" in list(df):
            df["firstdose"] += df["singledose"]

        out = self._rename_or_add_date_and_location(
            df,
            location_name_column="county",
            location_names_to_drop=["Unknown"],
            timezone="US/Eastern",
            apply_title_case=False,
        )

        return self._reshape_variables(out, self.variables)
