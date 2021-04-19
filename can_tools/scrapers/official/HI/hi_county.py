import json
import re

import pandas as pd
import requests
import us
from bs4 import BeautifulSoup

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
    # https://public.tableau.com/shared/H33ZZ9HCC?:display_count=y&:origin=viz_share_link&:embed=y
    # Hawai'i https://public.tableau.com/shared/7TCBHC568?:display_count=y&:origin=viz_share_link&:embed=y
    filterFunctionName = "[sqlproxy.0td6cgz0bpiy7x131qvze0jvbqr1].[none:County:nk]"  # this is the name of the user action, not the filter function name. doesn't work
    baseurl = "https://public.tableau.com"
    viewPath = "HawaiiCOVID-19-VaccinationDashboard3/VACCINESBYCOUNTY"
    # baseurl = "https://public.tableau.com/shared/"
    # viewPath = "7TCBHC568"
    counties = ["Maui", "Hawaii", "Honolulu", "Kauai"]

    def fetch(self):

        results = {}
        for county in self.counties:
            # Get county data
            self.filterFunctionValue = county
            results[county] = self.get_tableau_view()
        return results

    def normalize(self, data):
        dfs = []
        for county in self.counties:

            df = data[county]["Cumulative Persons"]
            df.columns = [
                "total_vaccine_initiated",
                "alias",
                "total_vaccine_completed",
                "total_vaccines_administered",
                "sum_daily_count",
                "measure_name",
                "dt",
                "date",
            ]

            df["location_name"] = county
            df.dt = pd.to_datetime(df.dt)
            dfs.append(df)

        df = pd.concat(dfs)
        crename = {
            "total_vaccine_initiated": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "total_vaccine_completed": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "sum_daily_count": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        out = df.melt(id_vars=["location_name", "dt"], value_vars=crename.keys())
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()
        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]

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
