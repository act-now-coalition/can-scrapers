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
        return engine.getWorkbook()

    def normalize(self, data) -> pd.DataFrame:
        return data