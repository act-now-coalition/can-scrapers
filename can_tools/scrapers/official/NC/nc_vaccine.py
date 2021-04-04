import pandas as pd
from pandas.core.frame import DataFrame
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS


class NCVaccine(TableauDashboard):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/Summary"

    data_tableau_table = "County Map"
    location_name_col = "County -alias"
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "SUM(Partially Vaccinated)-alias": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "SUM(Fully Vaccinated)-alias": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }

    def fetch(self):
        #county_data = self.get_tableau_view()

        """
        uses the tableauscraper module:
        https://github.com/bertrandmartel/tableau-scraping/blob/master/README.md
        """
        ts = TS()
        ts.loads("https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics")
        workbook = ts.getWorkbook()

        ageWeekly = workbook.getWorksheet("Age_Weekly_Statewide")
        raceWeekly = workbook.getWorksheet("Race Weekly Statewide")
        genderWeekly = workbook.getWorksheet("Gender_Weekly_statewide")
        ethnicityWeekly = workbook.getWorksheet("Ethnicity_Weekly_Statewide")

        frames = [ageWeekly.data, raceWeekly.data, genderWeekly.data, ethnicityWeekly.data]

        df = pd.concat(frames)

        df.to_csv('C:\personalproj\output.csv', sep=',')

        workbook = workbook.setParameter("Age_County", "Age_County")
        return workbook.worksheets[0].data
        print(county_data)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().normalize(df)
        df.location_name = df.location_name.str.replace(
            " County", ""
        ).str.strip()  # Strip whitespace
        df.loc[df["location_name"] == "Mcdowell", "location_name"] = "McDowell"
        return df
