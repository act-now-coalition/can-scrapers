import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper


class NCVaccine(TableauDashboard):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    location_type = "county"
    fetch_url = "https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/VaccinationDashboard"

    data_tableau_table = "County Map"
    location_name_col = "County-alias"
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "AGG(Calc.At Least One Dose Vaccinated)-alias": variables.INITIATING_VACCINATIONS_ALL,
        "AGG(Calc.Fully Vaccinated)-alias": variables.FULLY_VACCINATED_ALL,
        "AGG(Calc.Additional/Booster Dose)-alias": variables.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
    }

    def fetch(self):
        scraper_instance = TableauScraper()
        scraper_instance.loads(self.fetch_url)
        workbook = scraper_instance.getWorkbook()
        return workbook.getWorksheet("County Map").data

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().normalize(df)
        df.location_name = df.location_name.str.replace(
            " County", ""
        ).str.strip()  # Strip whitespace
        df.loc[df["location_name"] == "Mcdowell", "location_name"] = "McDowell"
        return df
