import us
import requests
import pandas as pd
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables


class MACountyVaccine(StateDashboard):

    source = (
        "https://www.mass.gov/info-details/"
        "covid-19-response-reporting#covid-19-daily-dashboard-"
    )
    state_fips = int(us.states.lookup("Massachusetts").fips)
    has_location = False

    variables = {
        "Fully vaccinated individuals": variables.FULLY_VACCINATED_ALL,
        "Individuals with at least one dose": variables.INITIATING_VACCINATIONS_ALL,
    }

    source_url = "https://www.mass.gov/doc/weekly-covid-19-municipality-vaccination-report-march-25-2021/download"

    def fetch(self):
        data = requests.get(self.source_url)
        return data.content

    def normalize(self, data):
        # preparing data frame to
        data = pd.read_excel(data, "Sex - zip code", header=[1, 2], index_col=0)
        data.columns = data.columns.set_names(["variable", "sex"])
        data.index = data.index.set_names(["zip"])
        data = data.stack(level="sex").reset_index()
        data = data.replace("*", 0)

        print(data)
        print(data)
        print(self._reshape_variables(data, self.variables))
