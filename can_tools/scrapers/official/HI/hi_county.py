import pandas as pd
import us
import requests
import re
import json
from bs4 import BeautifulSoup
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import (
    TableauDashboard,
    TableauMapClick,
    StateDashboard,
)

from can_tools.scrapers.util import requests_retry_session


class HawaiiVaccineCounty(TableauDashboard, StateDashboard):
    has_location = False
    source = "https://health.hawaii.gov/coronavirusdisease2019/what-you-should-know/current-situation-in-hawaii/#vaccine"
    source_name = (
        "State of Hawai'i - Department of Health"
        "Disease Outbreak Control Division | COVID-19"
    )
    location_type = "county"
    state_fips = int(us.states.lookup("Hawaii").fips)
    # https://public.tableau.com/shared/H33ZZ9HCC?:display_count=y&:origin=viz_share_link&:embed=y
    # Hawai'i https://public.tableau.com/shared/7TCBHC568?:display_count=y&:origin=viz_share_link&:embed=y
    filterFunctionName = f"county%20select" # this is the name of the user action, not the filter function name. doesn't work
    baseurl = "https://public.tableau.com"
    viewPath = "HawaiiCOVID-19-VaccinationDashboard/VACCINESBYCOUNTY"
    # baseurl = "https://public.tableau.com/shared/"
    # viewPath = "7TCBHC568"

    
    def fetch(self):
        # Get Maui County
        self.filterFunctionValue = 'MAUI'

        return self.get_tableau_view()

    def normalize(self, data):
        return data

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
