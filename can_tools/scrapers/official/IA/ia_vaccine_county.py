import pandas as pd
import us
import requests
import json
from io import StringIO
from can_tools.scrapers.official.base import StateDashboard, ScraperVariable
from can_tools.scrapers import variables


class IowaCountyVaccine(StateDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Iowa").fips)
    source = "https://coronavirus.iowa.gov/pages/vaccineinformation#VaccineInformation"
    source_name = "Iowa Department of Public Health"

    dashboard_link = "https://public.domo.com/embed/pages/1wB9j"
    card_id = "1179017552"
    csv_link = "https://public.domo.com/embed/pages/1wB9j/cards/1179017552/export"
    # Don't know if this will need to be renewed...
    # If it does, to get this token:
    # 1) open the dashboard_link in a browser
    # 2) Scroll down to the table titled Vaccine Series by County of Vaccine Provider (about 2/3 down)
    # 3) Hover over the table in the top right corner, a button should appear
    # 4) Inspect the Network tab in the browser, then click the link
    # 5) The token will be in the request headers under 'x-domo-embed-token`
    token = (
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjUyNjM5Mzc0IiwibmJmIjox"
        "NjE3MjgwNjQwLCJpc3MiOiJhcGlHYXRld2F5IiwiZW1iIjpbIntcInRva2VuXCI6XCIxd0I5a"
        "lwiLFwibGlua1R5cGVcIjpcIlNFQVJDSEFCTEVcIixcInBlcm1pc3Npb25zXCI6W1wiUkVBRFw"
        "iXX0iXSwiZXhwIjoxNjE3MzA5NDUwLCJpYXQiOjE2MTcyODA2NTAsImp0aSI6IjNkN2UwZTM2L"
        "WMyNTktNDlkOS04ZDdmLTlhYmVhMjU4ZTUzOSJ9.1_st_ky1-ZdDw3dO77I_qIVghNce6otO_00EUAuq7WE"
    )

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
        "total_administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        headers = {"x-domo-embed-token": self.token}
        res = requests.post(
            self.csv_link,
            {
                "request": json.dumps(
                    {
                        "fileName": "Vaccine Series by County of Vaccine Provider.csv",
                        "accept": "text/csv",
                        "type": "file",
                    }
                )
            },
            headers=headers,
        )
        data = StringIO(res.text)
        return pd.read_csv(data)

    def normalize(self, data):
        df = data.rename(
            columns={
                "Two-Dose Series Initiated": "total_vaccine_initiated",
                "Two-Dose Series Completed": "total_vaccine_completed",
                "Single-Dose Series Completed": "single_complete",
                "Total Doses Administered": "total_administered",
            }
        )

        df = self._rename_or_add_date_and_location(
            df,
            location_name_column="County",
            timezone="US/Central",
            apply_title_case=True,
            location_names_to_drop=["Out of State"],
        )
        # Count single dose vaccine as both initiated and completed
        df.total_vaccine_initiated += df.total_vaccine_completed + df.single_complete
        df.total_vaccine_completed += df.single_complete

        out = self._reshape_variables(df, self.variables)

        return out
