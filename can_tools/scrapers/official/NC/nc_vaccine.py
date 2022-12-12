import pandas as pd
import us
from typing import Dict

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper, TableauWorkbook


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


class NCVaccineState(NCVaccine):
    location_type = "state"
    has_location = True

    variables = {
        "First of Two": variables.INITIATING_VACCINATIONS_ALL,
        "Second of Two": variables.FULLY_VACCINATED_ALL,
        "Additional Dose": variables.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
    }

    def fetch(self) -> TableauWorkbook:
        scraper_instance = TableauScraper()
        scraper_instance.loads(self.fetch_url)
        return scraper_instance.getWorkbook()

    def normalize(self, workbook: TableauWorkbook) -> pd.DataFrame:
        data: Dict[str, int] = {}
        variables = ["Single Shot", "First of Two", "Second of Two", "Additional Dose"]
        for variable in variables:
            frame = workbook.getWorksheet(f"Total Card {variable}").data
            data[variable] = frame["AGG(Calc.Total Card Administered)-alias"].values[0]

        # add J&J doses to match our initiated and completed definitions
        data["First of Two"] += data["Single Shot"]
        data["Second of Two"] += data["Single Shot"]

        data = pd.DataFrame.from_dict(data, orient="index").reset_index()
        data.columns = ["variable", "value"]
        data = data.assign(
            dt=self._retrieve_dt(),
            vintage=self._retrieve_vintage(),
            location=self.state_fips,
        ).query("variable != 'Single Shot'")
        return self.extract_CMU(data, self.variables)
