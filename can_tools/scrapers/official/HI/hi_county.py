import json
import re

import pandas as pd
import requests
import us
from bs4 import BeautifulSoup

from can_tools.scrapers import variables as v
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard, TableauMapClick
from can_tools.scrapers.util import requests_retry_session


class HawaiiVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://health.hawaii.gov/coronavirusdisease2019/what-you-should-know/current-situation-in-hawaii/#vaccine"
    source_name = (
        "State of Hawai'i - Department of Health"
        "Disease Outbreak Control Division | COVID-19"
    )
    location_type = "county"
    state_fips = int(us.states.lookup("Hawaii").fips)
    baseurl = "https://public.tableau.com"
    viewPath = "HawaiiCOVID-19-VaccinationDashboard3/VACCINESBYCOUNTY"
    counties = ["Maui", "Hawaii", "Honolulu", "Kauai"]
    data_tableau_table = "Updated County Progress"

    variables = {
        "initiated": v.INITIATING_VACCINATIONS_ALL,
        "completed": v.FULLY_VACCINATED_ALL,
    }

    def normalize(self, data):

        # population is total county population
        df = (
            data.rename(
                columns={
                    f"AGG(TOTAL 1st doses)-alias": "initiated",
                    f"AGG(TOTAL 2nd doses)-alias": "completed",
                    "County Clean-alias": "location_name",
                }
            ).loc[
                :,
                ["initiated", "completed", "location_name", "Measure Names-alias"],
            ]
            # The data is repeated twice (with different values of measure name alias)
            # Slice the data to only get one instance of it
            .query("`Measure Names-alias` != 'Initiating'")
        )
        out = self._reshape_variables(df, self.variables)
        out["dt"] = self._retrieve_dt("US/Hawaii")
        return out

    def get_filters(self):
        url = f"{self.baseurl}/views/{self.viewPath}"
        print(url)
        req = requests_retry_session()
        # info, fdat = self.getRawTbluPageData(url, tbsroot, reqParams)
        r = req.get(
            url,
            params={
                ":language": "en",
                ":display_count": "y",
                ":origin": "viz_share_link",
                ":embed": "y",
                ":showVizHome": "n",
                ":jsdebug": "y",
                ":apiID": "host4",
                "#navType": "1",
                "navSrc": "Parse",
            },
            headers={"Accept": "text/javascript"},
        )

        soup = BeautifulSoup(r.text, "html.parser")
        # return soup
        tableau_data = json.loads(
            soup.find("textarea", {"id": "tsConfigContainer"}).text
        )

        # Call the bootstrapper: grab the state data, map selection update function
        dataUrl = f'{self.baseurl}{tableau_data["vizql_root"]}/bootstrapSession/sessions/{tableau_data["sessionid"]}'
        r = requests.post(
            dataUrl,
            data={
                "sheet_id": tableau_data["sheetId"],
                "showParams": tableau_data["showParams"],
            },
        )
        # Regex the non-json output
        dat = re.search("\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)
        # load info head and data group separately
        info = json.loads(dat.group(1))
        fdat = json.loads(dat.group(2))
        user_actions = info["worldUpdate"]["applicationPresModel"]["workbookPresModel"][
            "dashboardPresModel"
        ]["userActions"]
        return (info, fdat, user_actions)
