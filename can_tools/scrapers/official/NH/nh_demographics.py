from tableauscraper import TableauScraper
import us
import pandas as pd
from can_tools.scrapers import variables

from can_tools.scrapers.official.base import StateDashboard

class NHVaccineRace(StateDashboard):
    has_location = False
    source = "https://www.covid19.nh.gov/dashboard/vaccination"
    source_name = "New Hampshire DHHS"
    location_type = "county"

    state_fips = us.states.lookup("South Carolina").fips
    fetch_url = 'https://www.nh.gov/t/DHHS/views/VaccineOperationalDashboard/VaccineDashboard?&:isGuestRedirectFromVizportal=y&:embed=y'

    variables = {"total_vaccinations_initiated": variables.INITIATING_VACCINATIONS_ALL}

    def fetch(self):
        engine = TableauScraper()
        engine.loads(self.fetch_url)
        workbook = engine.getWorkbook()
        return workbook

        chart = workbook.goToSheet("Count: Bar Chart")
        return chart
        # return chart.getParameters()
        # engine = engine.getWorksheet("Count and Prop: Map (no R/E with town&rphn)")
        # selects = engine.getSelectableItems()
        # counties = [
        #     t["values"] for t in selects if t["column"] == "CMN + Town + RPHN"
        # ][0]

        workbook = engine.getWorksheet("Count: Bar Chart")
        workbook.getParameterVal
        workbook = workbook.select("Recipient County for maps", counties[2])
        county_data = workbook.getWorksheet("Count: Bar Chart").data
        return county_data
        

    def normalize(self, data) -> pd.DataFrame:
        return data