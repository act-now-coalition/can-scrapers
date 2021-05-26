import re
import json
from io import StringIO

import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import CMU, StateDashboard
from can_tools.scrapers.util import requests_retry_session


class IowaCountyVaccine(StateDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Iowa").fips)
    source = "https://coronavirus.iowa.gov/pages/vaccineinformation#VaccineInformation"
    source_name = "Iowa Department of Public Health"

    dashboard_link = "https://public.domo.com/embed/pages/1wB9j"
    card_id = "1179017552"
    csv_link = "https://public.domo.com/embed/pages/1wB9j/cards/1179017552/export"

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
        "total_administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
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
