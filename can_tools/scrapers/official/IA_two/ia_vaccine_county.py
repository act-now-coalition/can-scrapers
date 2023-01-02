import re
import json
from io import StringIO

import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import CMU, StateDashboard
from can_tools.scrapers.util import requests_retry_session

# this is a class that does stuff
class IowaCountyVaccineCopy(StateDashboard):
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Iowa").fips)
    source = "https://coronavirus.iowa.gov/pages/vaccineinformation#VaccineInformation"
    source_name = "Iowa Department of Public Health"

    dashboard_link = "https://public.domo.com/embed/pages/1wB9j"
    csv_link = "https://public.domo.com/embed/pages/1wB9j/cards/1698180896/export"

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
    }
    _req = None

    @property
    def req(self):
        if self._req is not None:
            return self._req
        return self._make_req()

    def _make_req(self):
        self._req = requests_retry_session()
        return self._req

    def _get_token(self):
        res_token = self.req.get(self.dashboard_link)
        return re.search(r"'x-domo-embed-token': '(.*?)'", res_token.text).group(1)

    def fetch(self):
        headers = {"x-domo-embed-token": self._get_token()}
        res = self.req.post(
            self.csv_link,
            {
                "request": json.dumps(
                    {
                        "fileName": "Total Doses Administered by Recipient County of Residence.csv",
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
                "First Dose Given": "total_vaccine_initiated",
                "First Dose Given (Merged from datasource: ec049a78-938f-43ce-8c53-e977ad7594bd)": "total_vaccine_initiated",
                "2-Dose Vaccinations Completed": "total_vaccine_completed",
                "2-Dose Vaccinations Completed (Merged from datasource: ec049a78-938f-43ce-8c53-e977ad7594bd)": "total_vaccine_completed",
                "1-Dose Vaccinations Completed": "single_complete",
                "1-Dose Vaccinations Completed (Merged from datasource: ec049a78-938f-43ce-8c53-e977ad7594bd)": "single_complete",
            }
        ).dropna()

        df = self._rename_or_add_date_and_location(
            df,
            location_column="RECIP_ADDRESS_COUNTY",
            timezone="US/Central",
        )
        # Count single dose vaccine as both initiated and completed
        df.total_vaccine_initiated += df.total_vaccine_completed + df.single_complete
        df.total_vaccine_completed += df.single_complete

        out = df.pipe(self._reshape_variables, variable_map=self.variables).assign(
            location=lambda x: x["location"].astype(int)
        )

        return out
